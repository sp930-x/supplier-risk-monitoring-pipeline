"""Daily Airflow DAG for the Supplier Risk Monitoring Pipeline.

This DAG runs one batch date at a time:
generate synthetic data -> validate it -> load raw Snowflake tables -> run dbt -> test dbt.

Airflow runtime notes:
- Mount this project directory into the Airflow environment.
- Set SUPPLIER_RISK_PROJECT_ROOT if Airflow cannot infer the project root from this file.
- Configure Snowflake authentication for the Airflow runtime environment before enabling the DAG.
  The load task expects the required Snowflake variables or secrets to be available there.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

try:
    from airflow.sdk import DAG
except ImportError:  # Airflow 2 fallback
    from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2 fallback
    from airflow.operators.bash import BashOperator


DAG_ID = "supplier_risk_monitoring_pipeline"

# Airflow may only mount the dags folder. In that case, set this environment
# variable to the real project root so the relative commands below can run.
PROJECT_ROOT = Path(
    os.getenv(
        "SUPPLIER_RISK_PROJECT_ROOT",
        Path(__file__).resolve().parents[1],
    )
)


with DAG(
    dag_id=DAG_ID,
    description="Generate, validate, load, and transform daily supplier risk data.",
    schedule="@daily",
    start_date=datetime(2026, 6, 10),
    # catchup=True tells Airflow to create one run per historical day since
    # start_date, which is useful for backfilling daily monitoring batches.
    catchup=True,
    max_active_runs=1,
    tags=["supplier-risk", "snowflake", "dbt"],
) as dag:
    generate_synthetic_batch = BashOperator(
        task_id="generate_synthetic_batch",
        cwd=str(PROJECT_ROOT),
        # {{ ds }} is Airflow's logical date formatted as YYYY-MM-DD.
        # We use it as the pipeline batch_date so every DAG run processes one day.
        bash_command="""
python scripts/generate_synthetic_olist_events.py \
  --olist-dir data/raw/olist \
  --output-dir data/generated \
  --start-date {{ ds }} \
  --days 1 \
  --min-orders-per-day 70 \
  --max-orders-per-day 130 \
  --seed 42 \
  --overwrite
""",
    )

    validate_generated_batch = BashOperator(
        task_id="validate_generated_batch",
        cwd=str(PROJECT_ROOT),
        bash_command="""
python scripts/validate_generated_data.py \
  --generated-dir data/generated \
  --batch-date {{ ds }}
""",
    )

    load_raw_to_snowflake = BashOperator(
        task_id="load_raw_to_snowflake",
        cwd=str(PROJECT_ROOT),
        bash_command="""
python scripts/load_raw_to_snowflake.py \
  --batch-date {{ ds }}
""",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        cwd=str(PROJECT_ROOT),
        bash_command="dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        cwd=str(PROJECT_ROOT),
        bash_command="dbt test",
    )

    generate_synthetic_batch >> validate_generated_batch >> load_raw_to_snowflake >> dbt_run >> dbt_test
