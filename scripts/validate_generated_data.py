import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd


def fail(message: str) -> None:
    raise ValueError(f"Validation failed: {message}")


def validate_required_columns(df: pd.DataFrame, required_columns: list[str], table_name: str) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        fail(f"{table_name} is missing columns: {missing}")


def validate_batch(batch_dir: Path) -> None:
    if not batch_dir.exists():
        fail(f"Batch directory does not exist: {batch_dir}")

    batch_date = batch_dir.name.replace("batch_date=", "")

    orders_path = batch_dir / "orders.csv"
    order_items_path = batch_dir / "order_items.csv"
    payments_path = batch_dir / "payments.csv"
    reviews_path = batch_dir / "reviews.csv"

    for path in [orders_path, order_items_path, payments_path, reviews_path]:
        if not path.exists():
            fail(f"Missing file: {path}")

    orders = pd.read_csv(orders_path)
    order_items = pd.read_csv(order_items_path)
    payments = pd.read_csv(payments_path)
    reviews = pd.read_csv(reviews_path)

    validate_required_columns(
        orders,
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "source_type",
            "batch_date",
        ],
        "orders",
    )

    validate_required_columns(
        order_items,
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value",
            "source_type",
            "batch_date",
        ],
        "order_items",
    )

    validate_required_columns(
        payments,
        [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_value",
            "source_type",
            "batch_date",
        ],
        "payments",
    )

    validate_required_columns(
        reviews,
        [
            "review_id",
            "order_id",
            "review_score",
            "review_creation_date",
            "source_type",
            "batch_date",
        ],
        "reviews",
    )

    # 1. orders.order_id must be unique
    if orders["order_id"].isna().any():
        fail("orders.order_id contains null values")

    duplicate_orders = orders["order_id"].duplicated().sum()
    if duplicate_orders > 0:
        fail(f"orders.order_id contains {duplicate_orders} duplicates")

    order_ids = set(orders["order_id"])

    # 2. order_items must reference existing orders
    missing_order_item_orders = set(order_items["order_id"]) - order_ids
    if missing_order_item_orders:
        fail(f"order_items contains order_id values not found in orders: {len(missing_order_item_orders)}")

    # 3. payments must reference existing orders
    missing_payment_orders = set(payments["order_id"]) - order_ids
    if missing_payment_orders:
        fail(f"payments contains order_id values not found in orders: {len(missing_payment_orders)}")

    # 4. reviews must reference existing orders
    missing_review_orders = set(reviews["order_id"]) - order_ids
    if missing_review_orders:
        fail(f"reviews contains order_id values not found in orders: {len(missing_review_orders)}")

    # 5. payment_sequential validation
    if payments["payment_sequential"].isna().any():
        fail("payments.payment_sequential contains null values")

    if (payments["payment_sequential"] < 1).any():
        fail("payments.payment_sequential must be >= 1")

    duplicate_payment_sequence = payments.duplicated(
        subset=["order_id", "payment_sequential"]
    ).sum()

    if duplicate_payment_sequence > 0:
        fail(f"Duplicate (order_id, payment_sequential) found: {duplicate_payment_sequence}")

    # Optional but useful: payment sequence should start at 1 for every order
    min_seq_by_order = payments.groupby("order_id")["payment_sequential"].min()
    invalid_min_seq = (min_seq_by_order != 1).sum()
    if invalid_min_seq > 0:
        fail(f"{invalid_min_seq} orders do not start payment_sequential at 1")

    # Optional: payment sequences should be consecutive per order
    for order_id, group in payments.groupby("order_id"):
        seqs = sorted(group["payment_sequential"].astype(int).tolist())
        expected = list(range(1, len(seqs) + 1))
        if seqs != expected:
            fail(f"payment_sequential is not consecutive for order_id={order_id}: {seqs}")

    # 6. Review score validation
    if not reviews["review_score"].between(1, 5).all():
        fail("reviews.review_score must be between 1 and 5")

    # 7. Numeric value checks
    if (order_items["price"] < 0).any():
        fail("order_items.price contains negative values")

    if (order_items["freight_value"] < 0).any():
        fail("order_items.freight_value contains negative values")

    if (payments["payment_value"] < 0).any():
        fail("payments.payment_value contains negative values")

    # 8. Payment value should roughly match item price + freight per order
    item_totals = (
        order_items.assign(item_total=order_items["price"] + order_items["freight_value"])
        .groupby("order_id")["item_total"]
        .sum()
        .round(2)
    )

    payment_totals = payments.groupby("order_id")["payment_value"].sum().round(2)

    comparison = pd.concat(
        [item_totals.rename("item_total"), payment_totals.rename("payment_total")],
        axis=1,
    )

    comparison["diff"] = (comparison["item_total"] - comparison["payment_total"]).abs()

    invalid_payment_totals = comparison[comparison["diff"] > 0.01]
    if len(invalid_payment_totals) > 0:
        fail(f"Payment totals do not match item totals for {len(invalid_payment_totals)} orders")

    # 9. Date validation
    orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])
    orders["order_approved_at"] = pd.to_datetime(orders["order_approved_at"], errors="coerce")
    orders["order_delivered_carrier_date"] = pd.to_datetime(
        orders["order_delivered_carrier_date"],
        errors="coerce",
    )
    orders["order_delivered_customer_date"] = pd.to_datetime(
        orders["order_delivered_customer_date"],
        errors="coerce",
    )
    orders["order_estimated_delivery_date"] = pd.to_datetime(orders["order_estimated_delivery_date"])
    reviews["review_creation_date"] = pd.to_datetime(reviews["review_creation_date"], errors="coerce")
    reviews["review_answer_timestamp"] = pd.to_datetime(
        reviews["review_answer_timestamp"],
        errors="coerce",
    )
    batch_end = pd.Timestamp(batch_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    invalid_delivery_dates = (
        orders["order_delivered_customer_date"] < orders["order_purchase_timestamp"]
    ).sum()

    if invalid_delivery_dates > 0:
        fail(f"{invalid_delivery_dates} orders have delivery date before purchase timestamp")

    invalid_approval_dates = (
        orders["order_approved_at"].notna()
        & (orders["order_approved_at"] < orders["order_purchase_timestamp"])
    ).sum()

    if invalid_approval_dates > 0:
        fail(f"{invalid_approval_dates} orders have approval timestamp before purchase timestamp")

    invalid_carrier_before_purchase = (
        orders["order_delivered_carrier_date"].notna()
        & (orders["order_delivered_carrier_date"] < orders["order_purchase_timestamp"])
    ).sum()

    if invalid_carrier_before_purchase > 0:
        fail(f"{invalid_carrier_before_purchase} orders have carrier date before purchase timestamp")

    invalid_carrier_before_approval = (
        orders["order_delivered_carrier_date"].notna()
        & (
            orders["order_approved_at"].isna()
            | (orders["order_delivered_carrier_date"] < orders["order_approved_at"])
        )
    ).sum()

    if invalid_carrier_before_approval > 0:
        fail(f"{invalid_carrier_before_approval} orders have carrier date before approval timestamp")

    invalid_customer_before_carrier = (
        orders["order_delivered_customer_date"].notna()
        & (
            orders["order_delivered_carrier_date"].isna()
            | (orders["order_delivered_customer_date"] < orders["order_delivered_carrier_date"])
        )
    ).sum()

    if invalid_customer_before_carrier > 0:
        fail(f"{invalid_customer_before_carrier} orders have customer delivery before carrier date")

    future_purchases = (orders["order_purchase_timestamp"] > batch_end).sum()

    if future_purchases > 0:
        fail(f"{future_purchases} orders have purchase timestamp after batch_date")

    future_approvals = (orders["order_approved_at"] > batch_end).sum()

    if future_approvals > 0:
        fail(f"{future_approvals} orders have approval timestamp after batch_date")

    future_carrier_dates = (orders["order_delivered_carrier_date"] > batch_end).sum()

    if future_carrier_dates > 0:
        fail(f"{future_carrier_dates} orders have carrier delivery date after batch_date")

    future_deliveries = (
        orders["order_delivered_customer_date"].notna()
        & (orders["order_delivered_customer_date"] > batch_end)
    ).sum()

    if future_deliveries > 0:
        fail(f"{future_deliveries} orders have delivery date after batch_date")

    future_reviews = (reviews["review_creation_date"] > batch_end).sum()

    if future_reviews > 0:
        fail(f"{future_reviews} reviews have review_creation_date after batch_date")

    future_review_answers = (reviews["review_answer_timestamp"] > batch_end).sum()

    if future_review_answers > 0:
        fail(f"{future_review_answers} reviews have review_answer_timestamp after batch_date")

    invalid_review_answer_order = (
        reviews["review_answer_timestamp"].notna()
        & (reviews["review_answer_timestamp"] < reviews["review_creation_date"])
    ).sum()

    if invalid_review_answer_order > 0:
        fail(f"{invalid_review_answer_order} reviews have answer timestamp before review creation date")

    delivered_order_ids = set(
        orders.loc[orders["order_delivered_customer_date"].notna(), "order_id"]
    )
    undelivered_review_orders = set(reviews["order_id"]) - delivered_order_ids

    if undelivered_review_orders:
        fail(f"reviews exist for {len(undelivered_review_orders)} undelivered orders")

    invalid_delivered_status = (
        (orders["order_status"] == "delivered")
        & orders["order_delivered_customer_date"].isna()
    ).sum()

    if invalid_delivered_status > 0:
        fail(f"{invalid_delivered_status} delivered orders have no delivery date")

    invalid_delivered_without_carrier = (
        (orders["order_status"] == "delivered")
        & orders["order_delivered_carrier_date"].isna()
    ).sum()

    if invalid_delivered_without_carrier > 0:
        fail(f"{invalid_delivered_without_carrier} delivered orders have no carrier date")

    invalid_shipped_status = (
        (orders["order_status"] == "shipped")
        & (
            orders["order_delivered_carrier_date"].isna()
            | orders["order_delivered_customer_date"].notna()
        )
    ).sum()

    if invalid_shipped_status > 0:
        fail(f"{invalid_shipped_status} shipped orders have missing carrier date or customer delivery date")

    invalid_processing_status = (
        (orders["order_status"] == "processing")
        & (
            orders["order_delivered_carrier_date"].notna()
            | orders["order_delivered_customer_date"].notna()
        )
    ).sum()

    if invalid_processing_status > 0:
        fail(f"{invalid_processing_status} processing orders already have delivery dates")

    invalid_open_status = (
        (orders["order_status"] != "delivered")
        & orders["order_delivered_customer_date"].notna()
    ).sum()

    if invalid_open_status > 0:
        fail(f"{invalid_open_status} open orders have delivery dates")

    # 10. batch_date validation
    for table_name, df in {
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
        "reviews": reviews,
    }.items():
        if "batch_date" not in df.columns:
            fail(f"{table_name} has no batch_date column")

        invalid_batch_dates = (df["batch_date"].astype(str) != batch_date).sum()
        if invalid_batch_dates > 0:
            fail(f"{table_name} has {invalid_batch_dates} rows with incorrect batch_date")

        invalid_source_type = (df["source_type"] != "synthetic").sum()
        if invalid_source_type > 0:
            fail(f"{table_name} has {invalid_source_type} rows where source_type is not synthetic")

    print(f"Validation passed for {batch_dir}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--generated-dir",
        default="data/generated",
        help="Directory containing batch_date=YYYY-MM-DD folders",
    )
    parser.add_argument(
        "--batch-date",
        help="Validate only this batch date in YYYY-MM-DD format.",
    )
    args = parser.parse_args()

    generated_dir = Path(args.generated_dir)

    if not generated_dir.is_dir():
        fail(f"Generated data directory does not exist: {generated_dir}")

    if args.batch_date:
        try:
            parsed_batch_date = datetime.strptime(args.batch_date, "%Y-%m-%d")
        except ValueError as exc:
            fail("--batch-date must use YYYY-MM-DD format")

        if parsed_batch_date.strftime("%Y-%m-%d") != args.batch_date:
            fail("--batch-date must use YYYY-MM-DD format")

        validate_batch(generated_dir / f"batch_date={args.batch_date}")
        print(f"Batch {args.batch_date} passed validation.")
        return

    batch_dirs = sorted(
        [path for path in generated_dir.iterdir() if path.is_dir() and path.name.startswith("batch_date=")]
    )

    if not batch_dirs:
        fail(f"No batch directories found in {generated_dir}")

    for batch_dir in batch_dirs:
        validate_batch(batch_dir)

    print("All generated batches passed validation.")


if __name__ == "__main__":
    main()
