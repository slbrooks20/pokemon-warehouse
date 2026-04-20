"""
ingestion/pokeapi/ingest_pokeapi.py
------------------------------------
Ingests raw JSON responses from PokéAPI into BigQuery (dataset: `raw`).

Endpoints covered:
  - /pokemon          → raw_pokemon
  - /move             → raw_moves
  - /type             → raw_types
  - /ability          → raw_abilities
  - /evolution-chain  → raw_evolutions

Design rules (from CLAUDE.md):
  - 0.5 s polite delay between every request
  - Exponential backoff on 429 / 5xx (up to MAX_RETRIES attempts)
  - Raw JSON loaded as-is — no transformation during ingestion
  - Partial failures are logged and skipped; the run does not crash
  - Log row counts on completion

Usage:
  python ingestion/pokeapi/ingest_pokeapi.py [--project GCP_PROJECT] [--dataset raw]

Credentials:
  Uses GCP Application Default Credentials (ADC).
  Run `gcloud auth application-default login` before executing.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Generator

import requests
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POKEAPI_BASE = "https://pokeapi.co/api/v2"
PAGE_SIZE = 100          # items per list request
POLITE_DELAY = 0.5       # seconds between every HTTP request
MAX_RETRIES = 5          # maximum retry attempts on transient errors
BACKOFF_BASE = 2.0       # exponential backoff base (seconds)

ENDPOINTS: dict[str, str] = {
    "pokemon": "raw_pokemon",
    "move": "raw_moves",
    "type": "raw_types",
    "ability": "raw_abilities",
    "evolution-chain": "raw_evolutions",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("ingest_pokeapi")


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class IngestionSummary:
    """Tracks per-endpoint counts and failures for the final log."""
    endpoint: str
    table: str
    rows_loaded: int = 0
    failed_urls: list[str] = field(default_factory=list)

    def log(self) -> None:
        status = "✓" if not self.failed_urls else "⚠"
        logger.info(
            "%s  %-20s → %-20s  rows=%d  failures=%d",
            status, self.endpoint, self.table,
            self.rows_loaded, len(self.failed_urls),
        )
        for url in self.failed_urls:
            logger.warning("    FAILED: %s", url)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_with_retry(
    session: requests.Session,
    url: str,
    *,
    max_retries: int = MAX_RETRIES,
    backoff_base: float = BACKOFF_BASE,
) -> dict[str, Any] | None:
    """
    GET *url* with exponential backoff on 429 / 5xx responses.

    Returns the parsed JSON dict, or None on permanent failure.
    Always sleeps POLITE_DELAY seconds before the *first* attempt so that
    callers don't need to remember to do it.
    """
    time.sleep(POLITE_DELAY)

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=30)

            if response.status_code == 200:
                return response.json()

            if response.status_code in (429, 500, 502, 503, 504):
                wait = backoff_base ** attempt
                logger.warning(
                    "HTTP %d for %s — retry %d/%d in %.1fs",
                    response.status_code, url, attempt, max_retries, wait,
                )
                time.sleep(wait)
                continue

            # 404 or other non-retryable error
            logger.error("HTTP %d (non-retryable) for %s", response.status_code, url)
            return None

        except requests.RequestException as exc:
            wait = backoff_base ** attempt
            logger.warning(
                "Request error for %s (%s) — retry %d/%d in %.1fs",
                url, exc, attempt, max_retries, wait,
            )
            time.sleep(wait)

    logger.error("Exhausted retries for %s", url)
    return None


def _paginate_list_endpoint(
    session: requests.Session,
    endpoint: str,
) -> Generator[dict[str, Any], None, None]:
    """
    Yield each item stub {name, url} from a paginated list endpoint.

    PokéAPI list responses look like:
        {"count": N, "next": "...", "results": [{name, url}, ...]}
    """
    url: str | None = f"{POKEAPI_BASE}/{endpoint}?limit={PAGE_SIZE}&offset=0"

    while url:
        data = _get_with_retry(session, url)
        if data is None:
            logger.error("Failed to fetch list page: %s — stopping pagination", url)
            break

        results = data.get("results", [])
        logger.debug("  page %s → %d items", url, len(results))
        yield from results

        url = data.get("next")  # None when we've reached the last page


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _ensure_table(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
) -> bigquery.Table:
    """
    Create the raw table if it doesn't exist.

    Schema: a single JSON column `payload` (STRING) plus an ingestion
    timestamp.  We store the entire API response as a JSON string so the
    raw layer is a faithful copy of the source; dbt staging models handle
    parsing.
    """
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"

    schema = [
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("source_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
    ]

    table = bigquery.Table(full_table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    logger.info("Table ready: %s", full_table_id)
    return table


def _load_rows(
    client: bigquery.Client,
    table: bigquery.Table,
    rows: list[dict[str, Any]],
) -> list[dict]:
    """
    Load *rows* into *table* via a batch load job (free-tier compatible).

    Uses load_table_from_json instead of insert_rows_json — streaming inserts
    are not available on the BigQuery free tier / sandbox.

    Returns a list of error dicts (empty means success).
    Each row must already have `ingested_at`, `source_url`, and `payload`.
    """
    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_json(
        rows,
        table,
        job_config=job_config,
    )
    load_job.result()  # blocks until the job completes

    if load_job.errors:
        return load_job.errors
    return []


# ---------------------------------------------------------------------------
# Per-endpoint ingestion
# ---------------------------------------------------------------------------

def ingest_endpoint(
    session: requests.Session,
    client: bigquery.Client,
    dataset_id: str,
    endpoint: str,
    table_name: str,
    *,
    batch_size: int = 50,
) -> IngestionSummary:
    """
    Full ingestion for one PokéAPI endpoint.

    Steps:
      1. Paginate the list endpoint to collect all resource URLs.
      2. Fetch each individual resource URL.
      3. Stream rows into BigQuery in batches.
    """
    summary = IngestionSummary(endpoint=endpoint, table=table_name)
    table = _ensure_table(client, dataset_id, table_name)

    ingested_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    batch: list[dict[str, Any]] = []

    logger.info("Starting ingestion: /%s → %s", endpoint, table_name)

    for stub in _paginate_list_endpoint(session, endpoint):
        resource_url: str = stub["url"]
        payload = _get_with_retry(session, resource_url)

        if payload is None:
            summary.failed_urls.append(resource_url)
            continue

        batch.append({
            "ingested_at": ingested_at,
            "source_url": resource_url,
            "payload": payload,  # pass dict directly — load_table_from_json serialises it
        })

        if len(batch) >= batch_size:
            errors = _load_rows(client, table, batch)
            if errors:
                logger.error("BQ insert errors: %s", errors)
            summary.rows_loaded += len(batch)
            logger.info("  /%s: %d rows loaded so far …", endpoint, summary.rows_loaded)
            batch = []

    # Flush remaining rows
    if batch:
        errors = _load_rows(client, table, batch)
        if errors:
            logger.error("BQ insert errors (final batch): %s", errors)
        summary.rows_loaded += len(batch)

    return summary


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest PokéAPI data into BigQuery raw layer.",
    )
    parser.add_argument(
        "--project",
        default=None,
        help=(
            "GCP project ID. Defaults to the project inferred from ADC "
            "(GOOGLE_CLOUD_PROJECT env var or gcloud config)."
        ),
    )
    parser.add_argument(
        "--dataset",
        default="raw",
        help="BigQuery dataset to load into (default: raw).",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        choices=list(ENDPOINTS.keys()),
        help="Ingest only this endpoint (default: all endpoints).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Rows per BigQuery streaming insert batch (default: 50).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    endpoints_to_run = (
        {args.endpoint: ENDPOINTS[args.endpoint]}
        if args.endpoint
        else ENDPOINTS
    )

    logger.info("=== PokéAPI ingestion starting ===")
    logger.info("Project  : %s", args.project or "(from ADC)")
    logger.info("Dataset  : %s", args.dataset)
    logger.info("Endpoints: %s", list(endpoints_to_run.keys()))

    client = bigquery.Client(project=args.project)

    # Ensure the dataset exists
    dataset_ref = bigquery.Dataset(f"{client.project}.{args.dataset}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    logger.info("Dataset ready: %s.%s", client.project, args.dataset)

    session = requests.Session()
    session.headers.update({"User-Agent": "pokemon-warehouse/1.0 (portfolio project)"})

    summaries: list[IngestionSummary] = []

    for endpoint, table_name in endpoints_to_run.items():
        try:
            summary = ingest_endpoint(
                session=session,
                client=client,
                dataset_id=args.dataset,
                endpoint=endpoint,
                table_name=table_name,
                batch_size=args.batch_size,
            )
        except Exception as exc:  # noqa: BLE001
            # Catch-all so one broken endpoint never kills the rest
            logger.exception("Unexpected error ingesting /%s: %s", endpoint, exc)
            summary = IngestionSummary(endpoint=endpoint, table=table_name)

        summaries.append(summary)

    # -----------------------------------------------------------------------
    # Final report
    # -----------------------------------------------------------------------
    logger.info("")
    logger.info("=== Ingestion complete — summary ===")
    total_rows = 0
    total_failures = 0
    for s in summaries:
        s.log()
        total_rows += s.rows_loaded
        total_failures += len(s.failed_urls)

    logger.info("---")
    logger.info("Total rows loaded : %d", total_rows)
    logger.info("Total failed URLs : %d", total_failures)

    if total_failures:
        logger.warning(
            "%d resource(s) failed to ingest. Re-run with --endpoint "
            "to retry a specific endpoint.",
            total_failures,
        )


if __name__ == "__main__":
    main()
