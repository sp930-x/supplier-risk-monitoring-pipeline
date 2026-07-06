from __future__ import annotations

import argparse
import os
import platform
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - handled by dependency note in the CLI output
    load_dotenv = None

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
except ImportError as exc:  # pragma: no cover - depends on local environment
    raise ImportError(
        'Missing Snowflake dependencies. Install them with: '
        'pip install "snowflake-connector-python[pandas]" python-dotenv'
    ) from exc

from validate_generated_data import validate_batch


def patch_windows_store_python_libc_ver() -> None:
    """Avoid a Snowflake connector crash caused by the Windows Store Python shim."""
    if os.name != "nt":
        return

    original_libc_ver = platform.libc_ver

    def safe_libc_ver(executable: str | None = None, lib: str = "", version: str = ""):
        try:
            return original_libc_ver(executable, lib, version)
        except OSError:
            return "", ""

    platform.libc_ver = safe_libc_ver


patch_windows_store_python_libc_ver()


REQUIRED_FILES = {
    "RAW_SYNTHETIC_ORDERS": "orders.csv",
    "RAW_SYNTHETIC_ORDER_ITEMS": "order_items.csv",
    "RAW_SYNTHETIC_PAYMENTS": "payments.csv",
    "RAW_SYNTHETIC_REVIEWS": "reviews.csv",
}


TABLE_SCHEMAS = {
    "RAW_SYNTHETIC_ORDERS": """
        CREATE TABLE IF NOT EXISTS RAW_SYNTHETIC_ORDERS (
            ORDER_ID VARCHAR,
            CUSTOMER_ID VARCHAR,
            ORDER_STATUS VARCHAR,
            ORDER_PURCHASE_TIMESTAMP VARCHAR,
            ORDER_APPROVED_AT VARCHAR,
            ORDER_DELIVERED_CARRIER_DATE VARCHAR,
            ORDER_DELIVERED_CUSTOMER_DATE VARCHAR,
            ORDER_ESTIMATED_DELIVERY_DATE VARCHAR,
            SOURCE_TYPE VARCHAR,
            BATCH_DATE VARCHAR
        )
    """,
    "RAW_SYNTHETIC_ORDER_ITEMS": """
        CREATE TABLE IF NOT EXISTS RAW_SYNTHETIC_ORDER_ITEMS (
            ORDER_ID VARCHAR,
            ORDER_ITEM_ID NUMBER,
            PRODUCT_ID VARCHAR,
            SELLER_ID VARCHAR,
            SHIPPING_LIMIT_DATE VARCHAR,
            PRICE FLOAT,
            FREIGHT_VALUE FLOAT,
            SOURCE_TYPE VARCHAR,
            BATCH_DATE VARCHAR,
            SELLER_RISK_CLUSTER VARCHAR
        )
    """,
    "RAW_SYNTHETIC_PAYMENTS": """
        CREATE TABLE IF NOT EXISTS RAW_SYNTHETIC_PAYMENTS (
            ORDER_ID VARCHAR,
            PAYMENT_SEQUENTIAL NUMBER,
            PAYMENT_TYPE VARCHAR,
            PAYMENT_INSTALLMENTS NUMBER,
            PAYMENT_VALUE FLOAT,
            SOURCE_TYPE VARCHAR,
            BATCH_DATE VARCHAR
        )
    """,
    "RAW_SYNTHETIC_REVIEWS": """
        CREATE TABLE IF NOT EXISTS RAW_SYNTHETIC_REVIEWS (
            REVIEW_ID VARCHAR,
            ORDER_ID VARCHAR,
            REVIEW_SCORE NUMBER,
            REVIEW_COMMENT_TITLE VARCHAR,
            REVIEW_COMMENT_MESSAGE VARCHAR,
            REVIEW_CREATION_DATE VARCHAR,
            REVIEW_ANSWER_TIMESTAMP VARCHAR,
            SOURCE_TYPE VARCHAR,
            BATCH_DATE VARCHAR
        )
    """,
    "RAW_INGESTION_LOG": """
        CREATE TABLE IF NOT EXISTS RAW_INGESTION_LOG (
            BATCH_DATE VARCHAR,
            TABLE_NAME VARCHAR,
            ROW_COUNT NUMBER,
            STATUS VARCHAR,
            INGESTED_AT TIMESTAMP_NTZ
        )
    """,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and load one generated synthetic batch into Snowflake raw tables."
    )
    parser.add_argument(
        "--batch-date",
        required=True,
        help="Batch date to load in YYYY-MM-DD format, for example 2026-06-10.",
    )
    parser.add_argument(
        "--generated-dir",
        default="data/generated",
        help="Directory containing batch_date=YYYY-MM-DD folders.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip local generated-data validation before loading.",
    )
    return parser.parse_args()


def validate_batch_date(batch_date: str) -> None:
    try:
        parsed = datetime.strptime(batch_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("--batch-date must use YYYY-MM-DD format.") from exc

    if parsed.strftime("%Y-%m-%d") != batch_date:
        raise ValueError("--batch-date must use YYYY-MM-DD format.")


def resolve_batch_dir(generated_dir: str, batch_date: str) -> Path:
    batch_dir = Path(generated_dir) / f"batch_date={batch_date}"

    if not batch_dir.is_dir():
        raise FileNotFoundError(f"Batch directory does not exist: {batch_dir}")

    missing_files = [
        file_name
        for file_name in REQUIRED_FILES.values()
        if not (batch_dir / file_name).is_file()
    ]
    if missing_files:
        raise FileNotFoundError(
            f"Batch directory {batch_dir} is missing required files: {missing_files}"
        )

    return batch_dir


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable: {name}. "
            "Set it in your shell or add it to a .env file."
        )
    return value


def connect_to_snowflake() -> snowflake.connector.SnowflakeConnection:
    if load_dotenv is not None:
        load_dotenv()

    connection_args = {
        "account": get_required_env("SNOWFLAKE_ACCOUNT"),
        "user": get_required_env("SNOWFLAKE_USER"),
        "role": get_required_env("SNOWFLAKE_ROLE"),
        "warehouse": get_required_env("SNOWFLAKE_WAREHOUSE"),
        "database": get_required_env("SNOWFLAKE_DATABASE"),
        "schema": get_required_env("SNOWFLAKE_SCHEMA"),
    }

    authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")
    if authenticator:
        connection_args["authenticator"] = authenticator
    else:
        connection_args["password"] = get_required_env("SNOWFLAKE_PASSWORD")

    return snowflake.connector.connect(
        **connection_args,
    )


def create_raw_tables(cursor: snowflake.connector.cursor.SnowflakeCursor) -> None:
    for create_statement in TABLE_SCHEMAS.values():
        cursor.execute(create_statement)


def read_csv_for_snowflake(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # Snowflake stores unquoted identifiers in uppercase. Matching that here
    # makes the loaded DataFrames line up cleanly with the raw table columns.
    df.columns = [column.upper() for column in df.columns]
    return df


def delete_existing_batch_rows(
    cursor: snowflake.connector.cursor.SnowflakeCursor,
    batch_date: str,
) -> None:
    for table_name in REQUIRED_FILES:
        cursor.execute(f"DELETE FROM {table_name} WHERE BATCH_DATE = %s", (batch_date,))

    cursor.execute("DELETE FROM RAW_INGESTION_LOG WHERE BATCH_DATE = %s", (batch_date,))


def load_dataframe(
    connection: snowflake.connector.SnowflakeConnection,
    table_name: str,
    df: pd.DataFrame,
) -> int:
    success, _num_chunks, num_rows, output = write_pandas(
        conn=connection,
        df=df,
        table_name=table_name,
        quote_identifiers=False,
    )

    if not success:
        raise RuntimeError(f"write_pandas failed for {table_name}. Snowflake output: {output}")

    return int(num_rows)


def write_ingestion_log(
    connection: snowflake.connector.SnowflakeConnection,
    batch_date: str,
    loaded_counts: dict[str, int],
) -> None:
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

    log_df = pd.DataFrame(
        [
            {
                "BATCH_DATE": batch_date,
                "TABLE_NAME": table_name,
                "ROW_COUNT": row_count,
                "STATUS": "SUCCESS",
                "INGESTED_AT": ingested_at,
            }
            for table_name, row_count in loaded_counts.items()
        ]
    )

    load_dataframe(connection, "RAW_INGESTION_LOG", log_df)


def main() -> None:
    args = parse_args()
    validate_batch_date(args.batch_date)

    batch_dir = resolve_batch_dir(args.generated_dir, args.batch_date)
    print(f"Loading generated batch {args.batch_date} from {batch_dir}")

    if args.skip_validation:
        print("Skipping validation because --skip-validation was provided.")
    else:
        print("Validating generated data before ingestion...")
        validate_batch(batch_dir)

    dataframes = {
        table_name: read_csv_for_snowflake(batch_dir / file_name)
        for table_name, file_name in REQUIRED_FILES.items()
    }

    connection = connect_to_snowflake()

    try:
        connection.autocommit(False)
        cursor = connection.cursor()

        try:
            create_raw_tables(cursor)
            delete_existing_batch_rows(cursor, args.batch_date)

            loaded_counts = {}
            for table_name, df in dataframes.items():
                row_count = load_dataframe(connection, table_name, df)
                loaded_counts[table_name] = row_count
                print(f"Loaded {row_count} rows into {table_name}")

            write_ingestion_log(connection, args.batch_date, loaded_counts)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
    finally:
        connection.close()

    print(f"Ingestion completed successfully for batch {args.batch_date}.")


if __name__ == "__main__":
    main()
