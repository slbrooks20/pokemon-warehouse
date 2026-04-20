"""
dags/pokemon_warehouse_dag.py
------------------------------
Monthly Airflow DAG for the Pokémon competitive meta warehouse.

Schedule: runs on the 7th of each month (Smogon publishes stats ~5th).

Task flow:
  ingest_smogon → ingest_sets → dbt_run → dbt_test

Assumes:
  - Python scripts are at /opt/airflow/ingestion/
  - dbt project is at /opt/airflow/dbt/
  - GCP credentials available via ADC (mounted service account key)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# DAG configuration
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

INGESTION_DIR = "/opt/airflow/ingestion"
DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="pokemon_warehouse_monthly",
    default_args=default_args,
    description="Monthly refresh of Pokémon competitive meta warehouse",
    schedule_interval="0 6 7 * *",  # 6 AM UTC on the 7th of each month
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pokemon", "warehouse", "smogon"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: Ingest Smogon chaos stats (all gens, latest month only)
    # -----------------------------------------------------------------------
    ingest_smogon = BashOperator(
        task_id="ingest_smogon",
        bash_command=f"python {INGESTION_DIR}/smogon/ingest_smogon_chaos.py --months 1",
        execution_timeout=timedelta(hours=2),
    )

    # -----------------------------------------------------------------------
    # Task 2: Ingest pkmn.cc recommended sets
    # -----------------------------------------------------------------------
    ingest_sets = BashOperator(
        task_id="ingest_sets",
        bash_command=f"python {INGESTION_DIR}/pkmn_cc/ingest_sets.py",
        execution_timeout=timedelta(minutes=10),
    )

    # -----------------------------------------------------------------------
    # Task 3: dbt run (rebuild staging → intermediate → marts)
    # -----------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt deps && dbt run",
        execution_timeout=timedelta(minutes=30),
    )

    # -----------------------------------------------------------------------
    # Task 4: dbt test (validate data quality)
    # -----------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
        execution_timeout=timedelta(minutes=15),
    )

    # -----------------------------------------------------------------------
    # Dependencies
    # -----------------------------------------------------------------------
    ingest_smogon >> ingest_sets >> dbt_run >> dbt_test
