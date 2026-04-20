"""
ingestion/smogon/ingest_smogon_chaos.py
---------------------------------------
Ingests Smogon chaos JSON stats into BigQuery (dataset: `raw`).

Tables populated:
  - raw_usage      — one row per (pokemon, tier, month)
  - raw_movesets   — one row per chaos file (full JSON payload)
  - raw_teammates  — one row per (pokemon, teammate, tier, month)

URL pattern: https://www.smogon.com/stats/YYYY-MM/chaos/gen{N}{tier}-1695.json
Tiers: ou, uu, ru, nu, pu, ubers, lc
Generations: 1–9, each targeting 12 months from its peak active era.

Design rules:
  - 404s are treated as "no data available" (info-level, not failures)
  - Actual failures (5xx, connection errors, BQ load errors) are logged and skipped
  - Batch load jobs (free-tier compatible, no streaming inserts)
  - Exponential backoff on transient HTTP errors

Usage:
  python ingestion/smogon/ingest_smogon_chaos.py [--project GCP_PROJECT] [--dataset raw]
  python ingestion/smogon/ingest_smogon_chaos.py --gen 9         # only gen 9
  python ingestion/smogon/ingest_smogon_chaos.py --gen 9 --months 3  # last 3 months of gen 9

Credentials:
  Uses GCP Application Default Credentials (ADC).
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

import requests
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SMOGON_STATS_BASE = "https://www.smogon.com/stats"
TIER_SUFFIXES = ["ou", "uu", "ru", "nu", "pu", "ubers", "lc"]

# OU uses 1695 as its standard rating cutoff; all other tiers use 1630
RATING_BY_TIER_SUFFIX: dict[str, str] = {
    "ou": "1695",
    "uu": "1630",
    "ru": "1630",
    "nu": "1630",
    "pu": "1630",
    "ubers": "1630",
    "lc": "1630",
}
MONTHS_PER_GEN = 12
MAX_RETRIES = 5
BACKOFF_BASE = 2.0
POLITE_DELAY = 1.0  # seconds between requests (Smogon has no formal rate limit docs)

# Peak active periods for each generation (approximate end-month of each gen's era).
# We take the 12 months ending at this date as the target window.
# For the current gen (9), we use the most recent 12 months dynamically.
GEN_ERA_END: dict[int, str] = {
    1: "2017-10",   # Gen 1 OU on Showdown — retro ladder, steady activity ~2017
    2: "2017-10",   # Gen 2 similar era
    3: "2017-10",   # Gen 3 similar era
    4: "2017-10",   # Gen 4 similar era
    5: "2017-10",   # Gen 5 similar era
    6: "2016-10",   # Gen 6 was current until Nov 2016 (Sun/Moon release)
    7: "2019-10",   # Gen 7 was current until Nov 2019 (Sword/Shield release)
    8: "2022-10",   # Gen 8 was current until Nov 2022 (Scarlet/Violet release)
    9: "latest",    # Gen 9 is current — use most recent months dynamically
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("ingest_smogon_chaos")

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class IngestionSummary:
    """Tracks per-table row counts, skips, and failures."""

    usage_rows: int = 0
    moveset_rows: int = 0
    teammate_rows: int = 0
    skipped: int = 0
    failures: list[str] = field(default_factory=list)

    def log(self) -> None:
        """Log final summary."""
        logger.info("  raw_usage     rows loaded: %d", self.usage_rows)
        logger.info("  raw_movesets  rows loaded: %d", self.moveset_rows)
        logger.info("  raw_teammates rows loaded: %d", self.teammate_rows)
        logger.info("  Skipped (no data/404): %d", self.skipped)
        if self.failures:
            logger.warning("  Real failures (%d):", len(self.failures))
            for f in self.failures:
                logger.warning("    %s", f)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


class NoDataAvailable(Exception):
    """Raised when a 404 indicates no data exists (not a real failure)."""


def _get_with_retry(
    session: requests.Session,
    url: str,
    *,
    max_retries: int = MAX_RETRIES,
    backoff_base: float = BACKOFF_BASE,
) -> requests.Response | None:
    """
    GET with exponential backoff. Returns Response or None on permanent failure.

    Raises NoDataAvailable on 404 (caller should treat as info, not error).
    """
    time.sleep(POLITE_DELAY)

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=60)

            if response.status_code == 200:
                return response

            if response.status_code == 404:
                raise NoDataAvailable(url)

            if response.status_code in (429, 500, 502, 503, 504):
                wait = backoff_base ** attempt
                logger.warning(
                    "HTTP %d for %s — retry %d/%d in %.1fs",
                    response.status_code, url, attempt, max_retries, wait,
                )
                time.sleep(wait)
                continue

            # Other non-retryable errors (403, etc.)
            logger.error("HTTP %d (non-retryable) for %s", response.status_code, url)
            return None

        except NoDataAvailable:
            raise
        except requests.RequestException as exc:
            wait = backoff_base ** attempt
            logger.warning(
                "Request error for %s (%s) — retry %d/%d in %.1fs",
                url, exc, attempt, max_retries, wait,
            )
            time.sleep(wait)

    logger.error("Exhausted retries for %s", url)
    return None


# ---------------------------------------------------------------------------
# Month discovery
# ---------------------------------------------------------------------------


def discover_months(session: requests.Session) -> list[str]:
    """
    Scrape the Smogon stats directory listing to find all available months.

    Returns all months sorted descending (most recent first).
    """
    resp = _get_with_retry(session, f"{SMOGON_STATS_BASE}/")
    if resp is None:
        logger.error("Could not fetch Smogon stats directory listing.")
        return []

    # The directory listing contains links like "2024-03/"
    months = re.findall(r'href="(\d{4}-\d{2})/"', resp.text)
    # Deduplicate and sort descending (most recent first)
    months = sorted(set(months), reverse=True)

    logger.info("Discovered %d total months available: %s … %s",
                len(months), months[0] if months else "N/A",
                months[-1] if months else "N/A")
    return months


def months_for_gen(gen: int, all_months: list[str], count: int = MONTHS_PER_GEN) -> list[str]:
    """
    Select the target months for a given generation.

    For the current gen (9), takes the most recent `count` months.
    For older gens, takes `count` months ending at their era's end date.
    """
    era_end = GEN_ERA_END[gen]

    if era_end == "latest":
        return all_months[:count]

    # Find months <= era_end
    eligible = [m for m in all_months if m <= era_end]
    return eligible[:count]


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------


def _ensure_table_usage(client: bigquery.Client, dataset_id: str) -> bigquery.Table:
    """Create raw_usage table if it doesn't exist."""
    full_id = f"{client.project}.{dataset_id}.raw_usage"
    schema = [
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("tier", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("month", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pokemon_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("usage_pct", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("rank", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("raw_count", "INT64", mode="REQUIRED"),
    ]
    table = bigquery.Table(full_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    return table


def _ensure_table_movesets(client: bigquery.Client, dataset_id: str) -> bigquery.Table:
    """Create raw_movesets table if it doesn't exist."""
    full_id = f"{client.project}.{dataset_id}.raw_movesets"
    schema = [
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("tier", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("month", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
    ]
    table = bigquery.Table(full_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    return table


def _ensure_table_teammates(client: bigquery.Client, dataset_id: str) -> bigquery.Table:
    """Create raw_teammates table if it doesn't exist."""
    full_id = f"{client.project}.{dataset_id}.raw_teammates"
    schema = [
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("tier", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("month", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pokemon_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("teammate_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("usage_rate", "FLOAT64", mode="REQUIRED"),
    ]
    table = bigquery.Table(full_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    return table


def _load_rows(
    client: bigquery.Client,
    table: bigquery.Table,
    rows: list[dict[str, Any]],
) -> list[dict]:
    """Batch load rows into a table. Returns error dicts (empty = success)."""
    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = client.load_table_from_json(rows, table, job_config=job_config)
    load_job.result()
    return load_job.errors or []


# ---------------------------------------------------------------------------
# Parsing and ingestion
# ---------------------------------------------------------------------------


def _parse_chaos_json(
    chaos_data: dict[str, Any],
    tier: str,
    month: str,
    source_url: str,
    ingested_at: str,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Parse a single chaos JSON file into rows for all three tables.

    Returns (usage_rows, moveset_rows, teammate_rows).
    """
    usage_rows: list[dict] = []
    teammate_rows: list[dict] = []

    data = chaos_data.get("data", {})

    # Sort by usage descending to assign rank
    sorted_pokemon = sorted(
        data.items(),
        key=lambda kv: kv[1].get("usage", 0),
        reverse=True,
    )

    for rank, (pokemon_name, poke_data) in enumerate(sorted_pokemon, start=1):
        usage_pct = poke_data.get("usage", 0.0)
        raw_count = poke_data.get("Raw count", 0)

        usage_rows.append({
            "ingested_at": ingested_at,
            "tier": tier,
            "month": month,
            "pokemon_name": pokemon_name,
            "usage_pct": usage_pct,
            "rank": rank,
            "raw_count": raw_count,
        })

        # Teammates
        teammates = poke_data.get("Teammates", {})
        for teammate_name, co_occurrence in teammates.items():
            teammate_rows.append({
                "ingested_at": ingested_at,
                "tier": tier,
                "month": month,
                "pokemon_name": pokemon_name,
                "teammate_name": teammate_name,
                "usage_rate": co_occurrence,
            })

    # Moveset row: one row with the full chaos payload
    moveset_rows = [{
        "ingested_at": ingested_at,
        "tier": tier,
        "month": month,
        "source_url": source_url,
        "payload": chaos_data,
    }]

    return usage_rows, moveset_rows, teammate_rows


def ingest_tier_month(
    session: requests.Session,
    client: bigquery.Client,
    tables: dict[str, bigquery.Table],
    tier: str,
    month: str,
    ingested_at: str,
    summary: IngestionSummary,
) -> None:
    """Fetch and load chaos data for a single (tier, month) combination."""
    # Determine rating suffix based on tier suffix (ou=1695, others=1630)
    tier_suffix = re.sub(r'^gen\d+', '', tier)
    rating = RATING_BY_TIER_SUFFIX.get(tier_suffix, "1630")
    url = f"{SMOGON_STATS_BASE}/{month}/chaos/{tier}-{rating}.json"

    try:
        resp = _get_with_retry(session, url)
    except NoDataAvailable:
        logger.debug("  No data: %s (404 — skipping)", url)
        summary.skipped += 1
        return

    if resp is None:
        summary.failures.append(url)
        return

    try:
        chaos_data = resp.json()
    except ValueError as exc:
        logger.error("Invalid JSON from %s: %s", url, exc)
        summary.failures.append(url)
        return

    usage_rows, moveset_rows, teammate_rows = _parse_chaos_json(
        chaos_data, tier, month, url, ingested_at,
    )

    # Load usage rows
    if usage_rows:
        errors = _load_rows(client, tables["usage"], usage_rows)
        if errors:
            logger.error("BQ errors loading raw_usage for %s/%s: %s", tier, month, errors)
            summary.failures.append(f"{url} (raw_usage load)")
        else:
            summary.usage_rows += len(usage_rows)

    # Load moveset rows
    if moveset_rows:
        errors = _load_rows(client, tables["movesets"], moveset_rows)
        if errors:
            logger.error("BQ errors loading raw_movesets for %s/%s: %s", tier, month, errors)
            summary.failures.append(f"{url} (raw_movesets load)")
        else:
            summary.moveset_rows += len(moveset_rows)

    # Load teammate rows in batches (can be large)
    batch_size = 5000
    for i in range(0, len(teammate_rows), batch_size):
        batch = teammate_rows[i:i + batch_size]
        errors = _load_rows(client, tables["teammates"], batch)
        if errors:
            logger.error("BQ errors loading raw_teammates batch for %s/%s: %s",
                         tier, month, errors)
            summary.failures.append(f"{url} (raw_teammates load batch {i})")
        else:
            summary.teammate_rows += len(batch)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Ingest Smogon chaos stats into BigQuery raw layer.",
    )
    parser.add_argument(
        "--project",
        default=None,
        help="GCP project ID. Defaults to ADC project.",
    )
    parser.add_argument(
        "--dataset",
        default="raw",
        help="BigQuery dataset to load into (default: raw).",
    )
    parser.add_argument(
        "--gen",
        type=int,
        default=None,
        choices=list(range(1, 10)),
        help="Ingest only this generation (default: all gens 1-9).",
    )
    parser.add_argument(
        "--tier-suffix",
        default=None,
        choices=TIER_SUFFIXES,
        help="Ingest only this tier suffix, e.g. 'ou' (default: all).",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=MONTHS_PER_GEN,
        help=f"Months per generation to ingest (default: {MONTHS_PER_GEN}).",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()

    gens = [args.gen] if args.gen else list(range(1, 10))
    suffixes = [args.tier_suffix] if args.tier_suffix else TIER_SUFFIXES

    logger.info("=== Smogon chaos ingestion starting ===")
    logger.info("Project     : %s", args.project or "(from ADC)")
    logger.info("Dataset     : %s", args.dataset)
    logger.info("Generations : %s", gens)
    logger.info("Tier suffixes: %s", suffixes)
    logger.info("Months/gen  : %d", args.months)

    client = bigquery.Client(project=args.project)

    # Ensure dataset exists
    dataset_ref = bigquery.Dataset(f"{client.project}.{args.dataset}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    # Ensure tables exist
    tables = {
        "usage": _ensure_table_usage(client, args.dataset),
        "movesets": _ensure_table_movesets(client, args.dataset),
        "teammates": _ensure_table_teammates(client, args.dataset),
    }
    logger.info("Tables ready.")

    session = requests.Session()
    session.headers.update({"User-Agent": "pokemon-warehouse/1.0 (portfolio project)"})

    # Discover all available months
    all_months = discover_months(session)
    if not all_months:
        logger.error("No months discovered — aborting.")
        return

    ingested_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    summary = IngestionSummary()

    for gen in gens:
        months = months_for_gen(gen, all_months, count=args.months)
        tiers = [f"gen{gen}{s}" for s in suffixes]

        logger.info("--- Gen %d: %d months (%s … %s), tiers: %s ---",
                    gen, len(months),
                    months[0] if months else "N/A",
                    months[-1] if months else "N/A",
                    tiers)

        for month in months:
            for tier in tiers:
                try:
                    ingest_tier_month(
                        session, client, tables, tier, month, ingested_at, summary,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "Unexpected error for %s/%s: %s — skipping", tier, month, exc,
                    )
                    summary.failures.append(f"{tier}/{month}: {exc}")

    # Final report
    logger.info("")
    logger.info("=== Smogon chaos ingestion complete ===")
    summary.log()
    logger.info("Total real failures: %d", len(summary.failures))


if __name__ == "__main__":
    main()
