"""Daily orchestration for the Supplier Risk Monitoring Pipeline.

Airflow prerequisites:
- Mount the project so this DAG can access the ``scripts`` directory.
- Install the Snowflake connector, pandas, python-dotenv, and dbt in Airflow.
- Configure Snowflake authentication in the Airflow runtime environment.
"""

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task


DAG_ID = "supplier_risk_daily_pipeline"
BATCH_DAYS = 1

# Override this in Airflow when only the dags directory is mounted.
PROJECT_ROOT = Path(
    os.getenv(
        "SUPPLIER_RISK_PROJECT_ROOT",
        Path(__file__).resolve().parents[1],
    )
)
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
OLIST_DIR = "data/raw/olist"
GENERATED_DIR = "data/generated"


def run_script(script_name: str, *arguments: str, env: dict | None = None) -> None:
    """Run a project script and fail the Airflow task if the script fails."""
    script_path = SCRIPTS_DIR / script_name
    if not script_path.is_file():
        raise FileNotFoundError(f"Required pipeline script not found: {script_path}")

    subprocess.run(
        [sys.executable, str(script_path), *arguments],
        cwd=PROJECT_ROOT,
        env=env,
        check=True,
    )


def run_command(*arguments: str) -> None:
    """Run a project command and fail the Airflow task if the command fails."""
    subprocess.run(
        list(arguments),
        cwd=PROJECT_ROOT,
        check=True,
    )


def batch_dates(start_date: str) -> list[str]:
    """Return the batch dates belonging to one daily data interval."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    return [
        (start + timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(BATCH_DAYS)
    ]


@dag(
    dag_id=DAG_ID,
    # Every morning at 09:00, process the completed previous calendar day.
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 6, 1, 9, tz="Europe/Berlin"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "supplier-risk",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["supplier-risk", "synthetic-data", "daily"],
    description="Generate, validate, and ingest one daily supplier risk batch.",
)
def supplier_risk_daily_pipeline():
    @task
    def generate_day(start_date: str) -> str:
        run_script(
            "generate_synthetic_olist_events.py",
            "--olist-dir",
            OLIST_DIR,
            "--output-dir",
            GENERATED_DIR,
            "--start-date",
            start_date,
            "--days",
            str(BATCH_DAYS),
            "--min-orders-per-day",
            "70",
            "--max-orders-per-day",
            "130",
            "--seed",
            "42",
            "--overwrite",
        )
        return start_date

    @task
    def validate_day(start_date: str) -> str:
        # Validate only the new daily batch, not all historical data.
        for batch_date in batch_dates(start_date):
            run_script(
                "validate_generated_data.py",
                "--generated-dir",
                GENERATED_DIR,
                "--batch-date",
                batch_date,
            )
        return start_date

    @task
    def load_day(start_date: str) -> None:
        for batch_date in batch_dates(start_date):
            run_script(
                "load_raw_to_snowflake.py",
                "--generated-dir",
                GENERATED_DIR,
                "--batch-date",
                batch_date,
            )

    @task
    def run_dbt_models() -> None:
        run_command("dbt", "run")

    @task
    def test_dbt_models() -> None:
        run_command("dbt", "test")

    # Use the completed previous day as the synthetic batch date.
    # This keeps both scheduled runs and manual UI triggers from creating today's partial data.
    # For a 2026-07-01 09:00 Europe/Berlin run, this creates batch_date=2026-06-30.
    batch_date = (
        "{{ (data_interval_end.in_timezone('Europe/Berlin') "
        "- macros.timedelta(days=1)) | ds }}"
    )
    generated_start = generate_day(batch_date)
    validated_start = validate_day(generated_start)
    loaded_start = load_day(validated_start)
    dbt_finished = run_dbt_models()
    loaded_start >> dbt_finished >> test_dbt_models()


supplier_risk_daily_pipeline()
