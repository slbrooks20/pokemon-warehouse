"""
ingestion/pkmn_cc/ingest_sets.py
---------------------------------
Fetches recommended sets from data.pkmn.cc and loads into BigQuery (dataset: `raw`).

Table populated:
  - raw_sets — single row with the full gen9 sets JSON blob

Source: https://data.pkmn.cc/sets/gen9.json

Usage:
  python ingestion/pkmn_cc/ingest_sets.py [--project GCP_PROJECT] [--dataset raw]

Credentials:
  Uses GCP Application Default Credentials (ADC).
"""

from __future__ import annotations

import argparse
import logging
import time

import requests
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SETS_URL = "https://data.pkmn.cc/sets/gen9.json"
MAX_RETRIES = 5
BACKOFF_BASE = 2.0

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("ingest_sets")

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _get_with_retry(
    session: requests.Session,
    url: str,
    *,
    max_retries: int = MAX_RETRIES,
    backoff_base: float = BACKOFF_BASE,
) -> requests.Response | None:
    """GET with exponential backoff. Returns Response or None on permanent failure."""
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=60)

            if response.status_code == 200:
                return response

            if response.status_code in (429, 500, 502, 503, 504):
                wait = backoff_base ** attempt
                logger.warning(
                    "HTTP %d for %s — retry %d/%d in %.1fs",
                    response.status_code, url, attempt, max_retries, wait,
                )
                time.sleep(wait)
                continue

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


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------


def _ensure_table(client: bigquery.Client, dataset_id: str) -> bigquery.Table:
    """Create raw_sets table if it doesn't exist."""
    full_id = f"{client.project}.{dataset_id}.raw_sets"
    schema = [
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("source_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
    ]
    table = bigquery.Table(full_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    logger.info("Table ready: %s", full_id)
    return table


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Ingest pkmn.cc recommended sets into BigQuery raw layer.",
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
    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()

    logger.info("=== pkmn.cc sets ingestion starting ===")
    logger.info("Project : %s", args.project or "(from ADC)")
    logger.info("Dataset : %s", args.dataset)
    logger.info("URL     : %s", SETS_URL)

    client = bigquery.Client(project=args.project)

    # Ensure dataset exists
    dataset_ref = bigquery.Dataset(f"{client.project}.{args.dataset}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    # Ensure table exists
    table = _ensure_table(client, args.dataset)

    session = requests.Session()
    session.headers.update({"User-Agent": "pokemon-warehouse/1.0 (portfolio project)"})

    # Fetch sets JSON
    logger.info("Fetching %s …", SETS_URL)
    resp = _get_with_retry(session, SETS_URL)
    if resp is None:
        logger.error("Failed to fetch sets data — aborting.")
        return

    try:
        sets_data = resp.json()
    except ValueError as exc:
        logger.error("Invalid JSON from %s: %s", SETS_URL, exc)
        return

    ingested_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    row = {
        "ingested_at": ingested_at,
        "source_url": SETS_URL,
        "payload": sets_data,
    }

    # Load into BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = client.load_table_from_json([row], table, job_config=job_config)
    load_job.result()

    if load_job.errors:
        logger.error("BQ load errors: %s", load_job.errors)
    else:
        logger.info("Successfully loaded 1 row into raw_sets.")

    logger.info("=== pkmn.cc sets ingestion complete ===")


if __name__ == "__main__":
    main()
