import argparse
import hashlib
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def deterministic_id(
    entity: str,
    batch_date: str,
    index: int,
    parent_id: str = "",
) -> str:
    value = f"{entity}:{batch_date}:{index}:{parent_id}"
    return str(uuid5(NAMESPACE_URL, value))


def stable_seed(base_seed: int, batch_date: str) -> int:
    value = f"{base_seed}:{batch_date}".encode("utf-8")
    return int.from_bytes(hashlib.md5(value).digest()[:8], byteorder="big")


def calculate_daily_order_count(
    batch_date: str,
    seed: int,
    orders_per_day: int,
    min_orders_per_day: int | None,
    max_orders_per_day: int | None,
) -> int:
    """
    Decide how many orders to generate for one batch date.

    If no min/max range is provided, keep the simple fixed-volume behavior.
    If a range is provided, use a deterministic random number for the date.
    """
    min_is_missing = min_orders_per_day is None
    max_is_missing = max_orders_per_day is None

    if min_is_missing and max_is_missing:
        return orders_per_day

    if min_is_missing or max_is_missing:
        raise ValueError(
            "Both --min-orders-per-day and --max-orders-per-day must be provided together."
        )

    if min_orders_per_day < 1 or max_orders_per_day < 1:
        raise ValueError("--min-orders-per-day and --max-orders-per-day must be at least 1.")

    if min_orders_per_day > max_orders_per_day:
        raise ValueError("--min-orders-per-day cannot be greater than --max-orders-per-day.")

    rng = np.random.default_rng(stable_seed(seed, f"{batch_date}:order_volume"))
    return int(rng.integers(min_orders_per_day, max_orders_per_day + 1))


def read_required_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")
    return pd.read_csv(path)


def format_datetime_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime(DATETIME_FORMAT)
    return df


def create_seller_risk_profiles(
    sellers: pd.DataFrame,
    seed: int = 42,
    high_risk_share: float = 0.12,
    medium_risk_share: float = 0.25,
) -> pd.DataFrame:
    """
    Create stable synthetic risk profiles for sellers.

    This makes some sellers naturally riskier than others, so the dashboard
    can show meaningful supplier risk patterns.
    """
    rng = np.random.default_rng(seed)
    seller_ids = sellers["seller_id"].dropna().unique()

    profiles = []

    for seller_id in seller_ids:
        r = rng.random()

        if r < high_risk_share:
            risk_cluster = "high"
            delay_probability = rng.uniform(0.35, 0.65)
            delay_severity = rng.uniform(3.0, 6.0)
        elif r < high_risk_share + medium_risk_share:
            risk_cluster = "medium"
            delay_probability = rng.uniform(0.15, 0.35)
            delay_severity = rng.uniform(2.0, 4.0)
        else:
            risk_cluster = "low"
            delay_probability = rng.uniform(0.03, 0.15)
            delay_severity = rng.uniform(1.0, 2.5)

        # Some sellers receive more orders than others.
        order_weight = rng.lognormal(mean=0.0, sigma=1.0)

        profiles.append(
            {
                "seller_id": seller_id,
                "risk_cluster": risk_cluster,
                "delay_probability": round(float(delay_probability), 4),
                "delay_severity": round(float(delay_severity), 4),
                "order_weight": round(float(order_weight), 4),
            }
        )

    profiles_df = pd.DataFrame(profiles)
    profiles_df["order_sampling_probability"] = (
        profiles_df["order_weight"] / profiles_df["order_weight"].sum()
    )

    return profiles_df


def pick_review_score(rng: np.random.Generator, delay_days: int) -> int:
    """
    Delay influences review score.
    This gives the synthetic data a realistic business relationship:
    late delivery usually increases the chance of low reviews.
    """
    if delay_days <= 0:
        return int(rng.choice([3, 4, 5], p=[0.12, 0.35, 0.53]))

    if 1 <= delay_days <= 3:
        return int(rng.choice([1, 2, 3, 4, 5], p=[0.08, 0.13, 0.27, 0.32, 0.20]))

    if 4 <= delay_days <= 7:
        return int(rng.choice([1, 2, 3, 4, 5], p=[0.18, 0.25, 0.30, 0.20, 0.07]))

    return int(rng.choice([1, 2, 3, 4, 5], p=[0.30, 0.30, 0.25, 0.12, 0.03]))


def sample_price_and_freight(
    rng: np.random.Generator,
    order_items_ref: pd.DataFrame,
) -> tuple[float, float]:
    """
    Sample realistic price and freight values from the original Olist order_items table.
    Falls back to generated values if reference data is missing or invalid.
    """
    if {"price", "freight_value"}.issubset(order_items_ref.columns) and len(order_items_ref) > 0:
        row = order_items_ref.sample(n=1, random_state=int(rng.integers(0, 1_000_000))).iloc[0]
        price = float(row["price"])
        freight = float(row["freight_value"])

        if price > 0 and freight >= 0:
            return round(price, 2), round(freight, 2)

    price = float(rng.uniform(20, 300))
    freight = float(rng.uniform(3, 40))
    return round(price, 2), round(freight, 2)


def generate_batch(
    batch_date: str,
    n_orders: int,
    sellers: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    order_items_ref: pd.DataFrame,
    seller_profiles: pd.DataFrame,
    seed: int,
) -> dict[str, pd.DataFrame]:
    """
    Generate one synthetic batch of Olist-like operational events.

    Output tables:
    - orders
    - order_items
    - payments
    - reviews
    """
    rng = np.random.default_rng(seed)
    batch_dt = datetime.strptime(batch_date, "%Y-%m-%d")
    snapshot_end = batch_dt + timedelta(days=1) - timedelta(seconds=1)

    seller_ids = seller_profiles["seller_id"].to_numpy()
    seller_probs = seller_profiles["order_sampling_probability"].to_numpy()

    customer_ids = customers["customer_id"].dropna().unique()
    product_ids = products["product_id"].dropna().unique()

    if len(customer_ids) == 0:
        raise ValueError("No customer_id values found in customers dataset.")

    if len(product_ids) == 0:
        raise ValueError("No product_id values found in products dataset.")

    orders_rows = []
    order_items_rows = []
    payments_rows = []
    reviews_rows = []

    profile_lookup = seller_profiles.set_index("seller_id").to_dict(orient="index")

    for order_idx in range(n_orders):
        order_id = deterministic_id("order", batch_date, order_idx)
        customer_id = str(rng.choice(customer_ids))
        seller_id = str(rng.choice(seller_ids, p=seller_probs))

        profile = profile_lookup[seller_id]
        delay_probability = float(profile["delay_probability"])
        delay_severity = float(profile["delay_severity"])
        risk_cluster = profile["risk_cluster"]

        estimated_delivery_days = int(rng.integers(3, 15))
        is_delivered = rng.random() < 0.72
        is_delayed = rng.random() < delay_probability

        if is_delayed:
            # Gamma distribution creates mostly small delays but sometimes larger delays.
            delay_days = max(1, int(rng.gamma(shape=2.0, scale=delay_severity)))
            delay_days = min(delay_days, 30)
        else:
            # Sometimes orders arrive earlier than estimated.
            delay_days = int(rng.choice([-3, -2, -1, 0], p=[0.10, 0.20, 0.30, 0.40]))

        actual_delivery_days = max(1, estimated_delivery_days + delay_days)

        if is_delivered:
            review_lag_days = int(rng.integers(1, 6))
            delivered_age_days = int(rng.integers(review_lag_days, review_lag_days + 15))
            delivered_customer_date = batch_dt - timedelta(days=delivered_age_days)
            delivered_customer_date = delivered_customer_date + timedelta(
                minutes=int(rng.integers(0, 1440))
            )
            purchase_ts = delivered_customer_date - timedelta(days=actual_delivery_days)
            order_status = "delivered"
        else:
            is_overdue_open = rng.random() < min(0.85, delay_probability + 0.20)

            if is_overdue_open:
                overdue_days = int(rng.integers(1, 10))
                estimated_delivery_date = batch_dt - timedelta(days=overdue_days)
                estimated_delivery_date = estimated_delivery_date + timedelta(
                    minutes=int(rng.integers(0, 1440))
                )
                order_status = "shipped"
            else:
                max_future_days = min(9, estimated_delivery_days)
                future_days = int(rng.integers(0, max_future_days + 1))
                estimated_delivery_date = batch_dt + timedelta(days=future_days)
                estimated_delivery_date = estimated_delivery_date + timedelta(
                    minutes=int(rng.integers(0, 1440))
                )
                order_status = str(rng.choice(["processing", "shipped"], p=[0.45, 0.55]))

            purchase_ts = estimated_delivery_date - timedelta(days=estimated_delivery_days)
            delivered_customer_date = None

        approved_at = min(
            purchase_ts + timedelta(hours=float(rng.uniform(0.2, 12))),
            snapshot_end,
        )
        delivered_carrier_date = purchase_ts + timedelta(days=int(rng.integers(1, 5)))
        estimated_delivery_date = purchase_ts + timedelta(days=estimated_delivery_days)

        if delivered_carrier_date > snapshot_end:
            delivered_carrier_date = None
        elif is_delivered and delivered_customer_date is not None and delivered_customer_date <= delivered_carrier_date:
            delivered_carrier_date = delivered_customer_date - timedelta(days=1)

        # One order can have multiple items, but keep it simple: same seller per order.
        n_items = int(rng.choice([1, 2, 3], p=[0.82, 0.15, 0.03]))

        total_payment_value = 0.0

        for item_id in range(1, n_items + 1):
            product_id = str(rng.choice(product_ids))
            price, freight = sample_price_and_freight(rng, order_items_ref)
            total_payment_value += price + freight

            order_items_rows.append(
                {
                    "order_id": order_id,
                    "order_item_id": item_id,
                    "product_id": product_id,
                    "seller_id": seller_id,
                    "shipping_limit_date": purchase_ts + timedelta(days=int(rng.integers(2, 6))),
                    "price": price,
                    "freight_value": freight,
                    "source_type": "synthetic",
                    "batch_date": batch_date,
                    "seller_risk_cluster": risk_cluster,
                }
            )

        payment_type = str(
            rng.choice(
                ["credit_card", "boleto", "voucher", "debit_card"],
                p=[0.72, 0.18, 0.06, 0.04],
            )
        )

        review_score = pick_review_score(rng, delay_days)

        orders_rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_status": order_status,
                "order_purchase_timestamp": purchase_ts,
                "order_approved_at": approved_at,
                "order_delivered_carrier_date": delivered_carrier_date,
                "order_delivered_customer_date": delivered_customer_date,
                "order_estimated_delivery_date": estimated_delivery_date,
                "source_type": "synthetic",
                "batch_date": batch_date,
            }
        )

        payments_rows.append(
            {
                "order_id": order_id,
                "payment_sequential": 1,
                "payment_type": payment_type,
                "payment_installments": int(rng.integers(1, 7)),
                "payment_value": round(total_payment_value, 2),
                "source_type": "synthetic",
                "batch_date": batch_date,
            }
        )

        if delivered_customer_date is not None:
            review_creation_date = delivered_customer_date + timedelta(
                days=int(rng.integers(1, 6))
            )
            review_answer_timestamp = review_creation_date + timedelta(days=int(rng.integers(0, 5)))

            if review_creation_date <= snapshot_end:
                reviews_rows.append(
                    {
                        "review_id": deterministic_id("review", batch_date, order_idx, order_id),
                        "order_id": order_id,
                        "review_score": review_score,
                        "review_comment_title": None,
                        "review_comment_message": None,
                        "review_creation_date": review_creation_date,
                        "review_answer_timestamp": min(review_answer_timestamp, snapshot_end),
                        "source_type": "synthetic",
                        "batch_date": batch_date,
                    }
                )

    orders = pd.DataFrame(orders_rows)
    order_items = pd.DataFrame(order_items_rows)
    payments = pd.DataFrame(payments_rows)
    reviews = pd.DataFrame(
        reviews_rows,
        columns=[
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
            "source_type",
            "batch_date",
        ],
    )

    orders = format_datetime_columns(
        orders,
        [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )

    order_items = format_datetime_columns(order_items, ["shipping_limit_date"])

    reviews = format_datetime_columns(
        reviews,
        ["review_creation_date", "review_answer_timestamp"],
    )

    return {
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
        "reviews": reviews,
    }


def save_batch(
    batch_data: dict[str, pd.DataFrame],
    output_dir: Path,
    batch_date: str,
    overwrite: bool = False,
) -> None:
    batch_dir = output_dir / f"batch_date={batch_date}"

    if batch_dir.exists() and not overwrite:
        print(f"Skipping batch {batch_date}: {batch_dir} already exists.")
        return

    batch_dir.mkdir(parents=True, exist_ok=True)

    for table_name, df in batch_data.items():
        output_path = batch_dir / f"{table_name}.csv"
        df.to_csv(output_path, index=False)

    print(f"Saved batch {batch_date} to {batch_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic Olist-like order, delivery, payment, and review events."
    )

    parser.add_argument(
        "--olist-dir",
        type=str,
        default="data/raw/olist",
        help="Directory containing original Olist CSV files.",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/generated",
        help="Directory where synthetic batches will be saved.",
    )

    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start batch date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of daily batches to generate.",
    )

    parser.add_argument(
        "--orders-per-day",
        type=int,
        default=200,
        help="Number of synthetic orders per daily batch.",
    )

    parser.add_argument(
        "--min-orders-per-day",
        type=int,
        default=None,
        help="Minimum synthetic orders per batch date when using variable daily volume.",
    )

    parser.add_argument(
        "--max-orders-per-day",
        type=int,
        default=None,
        help="Maximum synthetic orders per batch date when using variable daily volume.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite batch folders that already exist.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    olist_dir = Path(args.olist_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sellers = read_required_csv(olist_dir / "olist_sellers_dataset.csv")
    customers = read_required_csv(olist_dir / "olist_customers_dataset.csv")
    products = read_required_csv(olist_dir / "olist_products_dataset.csv")
    order_items_ref = read_required_csv(olist_dir / "olist_order_items_dataset.csv")

    required_columns = {
        "sellers": (sellers, ["seller_id"]),
        "customers": (customers, ["customer_id"]),
        "products": (products, ["product_id"]),
        "order_items": (order_items_ref, ["price", "freight_value"]),
    }

    for name, (df, cols) in required_columns.items():
        missing = [col for col in cols if col not in df.columns]
        if missing:
            raise ValueError(f"{name} dataset is missing required columns: {missing}")

    seller_profiles = create_seller_risk_profiles(
        sellers=sellers,
        seed=args.seed,
    )

    seller_profiles_path = output_dir / "seller_risk_profiles.csv"
    seller_profiles.to_csv(seller_profiles_path, index=False)
    print(f"Saved seller risk profiles to {seller_profiles_path}")

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")

    for day_offset in range(args.days):
        current_date = start_date + timedelta(days=day_offset)
        batch_date = current_date.strftime("%Y-%m-%d")
        batch_dir = output_dir / f"batch_date={batch_date}"

        if batch_dir.exists() and not args.overwrite:
            print(f"Skipping batch {batch_date}: {batch_dir} already exists.")
            continue

        batch_seed = stable_seed(args.seed, batch_date)
        daily_orders = calculate_daily_order_count(
            batch_date=batch_date,
            seed=args.seed,
            orders_per_day=args.orders_per_day,
            min_orders_per_day=args.min_orders_per_day,
            max_orders_per_day=args.max_orders_per_day,
        )

        print(f"Generating batch {batch_date} with {daily_orders} orders")

        batch_data = generate_batch(
            batch_date=batch_date,
            n_orders=daily_orders,
            sellers=sellers,
            customers=customers,
            products=products,
            order_items_ref=order_items_ref,
            seller_profiles=seller_profiles,
            seed=batch_seed,
        )

        save_batch(batch_data, output_dir, batch_date, overwrite=args.overwrite)

    print("Synthetic data generation completed.")


if __name__ == "__main__":
    main()
