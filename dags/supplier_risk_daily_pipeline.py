"""Daily orchestration for the Supplier Risk Monitoring Pipeline.

Airflow prerequisites:
- Mount the project so this DAG can access the ``scripts`` directory.
- Install the Snowflake connector, pandas, python-dotenv, and dbt in Airflow.
- Configure Snowflake authentication in the Airflow runtime environment.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


DAG_ID = "supplier_risk_daily_pipeline"
PROJECT_ROOT = "${SUPPLIER_RISK_PROJECT_ROOT:-/opt/airflow/project}"
BATCH_DATE = "{{ logical_date.in_timezone('Europe/Berlin') | ds }}"


def project_command(command: str) -> str:
    """Run commands from the mounted project root inside the Airflow container."""
    return f"cd {PROJECT_ROOT} && {command}"


with DAG(
    dag_id=DAG_ID,
    # The logical date is the batch date, so backfills create one batch per day.
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 6, 1, 3, tz="Europe/Berlin"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "supplier-risk",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["supplier-risk", "synthetic-data", "daily"],
    description="Generate, validate, and ingest one daily supplier risk batch.",
) as dag:
    generate_synthetic_batch = BashOperator(
        task_id="generate_synthetic_batch",
        bash_command=project_command(
            "python scripts/generate_synthetic_olist_events.py "
            "--olist-dir data/raw/olist "
            "--output-dir data/generated "
            f"--start-date {BATCH_DATE} "
            "--days 1 "
            "--min-orders-per-day 70 "
            "--max-orders-per-day 130 "
            "--seed 42 "
            "--overwrite"
        ),
    )

    validate_generated_batch = BashOperator(
        task_id="validate_generated_batch",
        bash_command=project_command(
            "python scripts/validate_generated_data.py "
            "--generated-dir data/generated "
            f"--batch-date {BATCH_DATE}"
        ),
    )

    load_raw_to_snowflake = BashOperator(
        task_id="load_raw_to_snowflake",
        bash_command=project_command(
            "python scripts/load_raw_to_snowflake.py "
            "--generated-dir data/generated "
            f"--batch-date {BATCH_DATE}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=project_command("dbt --no-partial-parse run"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=project_command("dbt --no-partial-parse test"),
    )

    (
        generate_synthetic_batch
        >> validate_generated_batch
        >> load_raw_to_snowflake
        >> dbt_run
        >> dbt_test
    )
