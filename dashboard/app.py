import os
from datetime import date

import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv


MART_TABLE = "SUPPLIER_RISK.ANALYTICS.MART_SUPPLIER_RISK_SNAPSHOT"
RISK_LEVEL_ORDER = ["high", "medium", "low"]
RISK_LEVEL_COLORS = {
    "high": "#d62728",
    "medium": "#ff7f0e",
    "low": "#2ca02c",
}


def get_required_env(name: str) -> str:
    """Read one required value from .env or the local shell environment."""
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable: {name}. "
            "Add it to your .env file before running the dashboard."
        )
    return value


def connect_to_snowflake() -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection using the same .env contract as the pipeline."""
    load_dotenv()

    return snowflake.connector.connect(
        account=get_required_env("SNOWFLAKE_ACCOUNT"),
        user=get_required_env("SNOWFLAKE_USER"),
        password=get_required_env("SNOWFLAKE_PASSWORD"),
        role=get_required_env("SNOWFLAKE_ROLE"),
        warehouse=get_required_env("SNOWFLAKE_WAREHOUSE"),
        database=get_required_env("SNOWFLAKE_DATABASE"),
    )


@st.cache_data(ttl=300)
def load_supplier_risk_data() -> pd.DataFrame:
    """Load the final dbt mart into a pandas DataFrame."""
    query = f"""
        select
            snapshot_date::date as "snapshot_date",
            seller_id as "seller_id",
            total_orders as "total_orders",
            late_delivery_rate as "late_delivery_rate",
            avg_delay_days as "avg_delay_days",
            avg_review_score as "avg_review_score",
            delayed_order_value as "delayed_order_value",
            risk_score as "risk_score",
            risk_level as "risk_level"
        from {MART_TABLE}
        order by snapshot_date, seller_id
    """

    connection = connect_to_snowflake()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            df = cursor.fetch_pandas_all()
        finally:
            cursor.close()
    finally:
        connection.close()

    # Keep dates and numeric fields predictable for filtering, KPIs, and charts.
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date

    numeric_columns = [
        "total_orders",
        "late_delivery_rate",
        "avg_delay_days",
        "avg_review_score",
        "delayed_order_value",
        "risk_score",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    df["risk_level"] = df["risk_level"].fillna("unknown").str.lower()

    return df


def format_currency(value: float) -> str:
    """Format a number as a simple currency-style value for Streamlit metrics."""
    return f"${value:,.2f}"


def get_ordered_risk_levels(df: pd.DataFrame) -> list[str]:
    """Return risk levels in a consistent business order."""
    existing_levels = set(df["risk_level"].unique())
    ordered_levels = [
        level for level in RISK_LEVEL_ORDER if level in existing_levels
    ]
    extra_levels = sorted(existing_levels - set(RISK_LEVEL_ORDER))
    return ordered_levels + extra_levels


def build_sidebar_filters(df: pd.DataFrame) -> tuple[date, list[str], int]:
    """Create sidebar filters and return the selected values."""
    st.sidebar.header("Filters")

    available_dates = sorted(df["snapshot_date"].unique())
    latest_snapshot_date = max(available_dates)
    latest_snapshot_index = available_dates.index(latest_snapshot_date)

    selected_snapshot_date = st.sidebar.selectbox(
        "snapshot_date",
        options=available_dates,
        index=latest_snapshot_index,
    )

    available_risk_levels = get_ordered_risk_levels(df)
    selected_risk_levels = st.sidebar.multiselect(
        "risk_level",
        options=available_risk_levels,
        default=available_risk_levels,
    )

    max_total_orders = int(df["total_orders"].max())
    default_min_total_orders = 3 if max_total_orders >= 3 else max_total_orders
    min_total_orders = st.sidebar.slider(
        "Minimum total orders",
        min_value=0,
        max_value=max_total_orders,
        value=default_min_total_orders,
        step=1,
    )

    return selected_snapshot_date, selected_risk_levels, min_total_orders


def show_kpis(df: pd.DataFrame) -> None:
    """Show KPI cards for the selected snapshot date."""
    supplier_snapshots = len(df)
    unique_suppliers = df["seller_id"].nunique()
    high_risk_suppliers = len(df[df["risk_level"] == "high"])
    average_risk_score = df["risk_score"].mean() if not df.empty else 0
    delayed_order_value = df["delayed_order_value"].sum()

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Supplier snapshots", f"{supplier_snapshots:,}")
    col2.metric("Unique suppliers", f"{unique_suppliers:,}")
    col3.metric("High-risk suppliers", f"{high_risk_suppliers:,}")
    col4.metric("Average risk score", f"{average_risk_score:.3f}")
    col5.metric("Delayed order value", format_currency(delayed_order_value))


def show_risk_distribution(df: pd.DataFrame) -> None:
    """Show the risk level mix for the selected snapshot date."""
    risk_distribution = (
        df.groupby("risk_level", as_index=False)
        .size()
        .rename(columns={"size": "supplier_snapshots"})
    )
    risk_distribution["risk_level"] = pd.Categorical(
        risk_distribution["risk_level"],
        categories=RISK_LEVEL_ORDER,
        ordered=True,
    )
    risk_distribution = risk_distribution.sort_values("risk_level")

    risk_distribution_chart = px.bar(
        risk_distribution,
        x="risk_level",
        y="supplier_snapshots",
        color="risk_level",
        title="Risk level distribution",
        color_discrete_map=RISK_LEVEL_COLORS,
        labels={
            "risk_level": "Risk level",
            "supplier_snapshots": "Supplier snapshots",
        },
    )
    st.plotly_chart(risk_distribution_chart, use_container_width=True)


def show_trend_charts(df: pd.DataFrame) -> None:
    """Build trend charts from all available snapshot dates."""
    all_snapshot_dates = pd.DataFrame(
        {"snapshot_date": sorted(df["snapshot_date"].unique())}
    )

    # Count high-risk suppliers per day and keep zero-count days visible.
    high_risk_over_time = (
        df[df["risk_level"] == "high"]
        .groupby("snapshot_date", as_index=False)
        .size()
        .rename(columns={"size": "high_risk_suppliers"})
    )
    high_risk_over_time = all_snapshot_dates.merge(
        high_risk_over_time,
        on="snapshot_date",
        how="left",
    ).fillna({"high_risk_suppliers": 0})

    high_risk_chart = px.line(
        high_risk_over_time,
        x="snapshot_date",
        y="high_risk_suppliers",
        markers=True,
        title="High-risk suppliers over time",
        labels={
            "snapshot_date": "Snapshot date",
            "high_risk_suppliers": "High-risk suppliers",
        },
    )
    st.plotly_chart(high_risk_chart, use_container_width=True)

    average_risk_score_over_time = df.groupby("snapshot_date", as_index=False)[
        "risk_score"
    ].mean()
    average_risk_score_chart = px.line(
        average_risk_score_over_time,
        x="snapshot_date",
        y="risk_score",
        markers=True,
        title="Average risk score over time",
        labels={
            "snapshot_date": "Snapshot date",
            "risk_score": "Average risk score",
        },
    )
    st.plotly_chart(average_risk_score_chart, use_container_width=True)

    delayed_order_value_over_time = df.groupby("snapshot_date", as_index=False)[
        "delayed_order_value"
    ].sum()
    delayed_order_value_chart = px.line(
        delayed_order_value_over_time,
        x="snapshot_date",
        y="delayed_order_value",
        markers=True,
        title="Delayed order value over time",
        labels={
            "snapshot_date": "Snapshot date",
            "delayed_order_value": "Delayed order value",
        },
    )
    st.plotly_chart(delayed_order_value_chart, use_container_width=True)


def show_suppliers_requiring_attention(df: pd.DataFrame) -> None:
    """Show high-risk suppliers that need business attention."""
    table_columns = [
        "snapshot_date",
        "seller_id",
        "total_orders",
        "late_delivery_rate",
        "avg_delay_days",
        "avg_review_score",
        "delayed_order_value",
        "risk_score",
        "risk_level",
    ]

    if df.empty:
        st.info("No high-risk suppliers match the current attention filters.")
        return

    attention_table = (
        df[table_columns]
        .sort_values(
            by=["risk_score", "delayed_order_value"],
            ascending=[False, False],
        )
        .reset_index(drop=True)
    )

    st.dataframe(
        attention_table,
        use_container_width=True,
        hide_index=True,
        column_config={
            "late_delivery_rate": st.column_config.NumberColumn(
                "late_delivery_rate",
                format="%.2f",
            ),
            "avg_delay_days": st.column_config.NumberColumn(
                "avg_delay_days",
                format="%.2f",
            ),
            "avg_review_score": st.column_config.NumberColumn(
                "avg_review_score",
                format="%.2f",
            ),
            "delayed_order_value": st.column_config.NumberColumn(
                "delayed_order_value",
                format="$%.2f",
            ),
            "risk_score": st.column_config.NumberColumn(
                "risk_score",
                format="%.3f",
            ),
        },
    )


def main() -> None:
    st.set_page_config(
        page_title="Supplier Risk Monitoring Dashboard",
        layout="wide",
    )

    st.title("Supplier Risk Monitoring Dashboard")
    st.write(
        "This dashboard monitors supplier risk using daily supplier risk snapshots. "
        "It helps identify suppliers with repeated delivery delays, low review scores, "
        "and high delayed order value."
    )
    st.info(
        "The dashboard reads from the final dbt mart table in Snowflake. "
        "The mart is refreshed by the Airflow pipeline."
    )

    try:
        df = load_supplier_risk_data()
    except Exception as exc:
        st.error(f"Could not load supplier risk data from Snowflake: {exc}")
        st.stop()

    if df.empty:
        st.warning(f"No rows found in {MART_TABLE}.")
        st.stop()

    (
        selected_snapshot_date,
        selected_risk_levels,
        min_total_orders,
    ) = build_sidebar_filters(df)

    selected_snapshot_df = df[df["snapshot_date"] == selected_snapshot_date]
    current_risk_df = selected_snapshot_df[
        selected_snapshot_df["risk_level"].isin(selected_risk_levels)
    ]

    if current_risk_df.empty:
        st.warning("No supplier risk snapshots match the selected filters.")
        st.stop()

    st.header("Current Risk Overview")
    st.caption(f"Selected snapshot date: {selected_snapshot_date}")
    show_kpis(current_risk_df)
    show_risk_distribution(current_risk_df)

    st.header("Risk Trends Over Time")
    show_trend_charts(df)

    st.header("Suppliers Requiring Attention")
    st.caption(
        "Showing high-risk suppliers on the selected snapshot date "
        f"with at least {min_total_orders} total orders."
    )
    attention_df = selected_snapshot_df[
        (selected_snapshot_df["risk_level"] == "high")
        & (selected_snapshot_df["total_orders"] >= min_total_orders)
    ]
    show_suppliers_requiring_attention(attention_df)


if __name__ == "__main__":
    main()
