import os
from datetime import date, timedelta

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
            overdue_open_orders as "overdue_open_orders",
            late_delivery_rate as "late_delivery_rate",
            avg_delay_days as "avg_delay_days",
            avg_review_score as "avg_review_score",
            reviewed_orders as "reviewed_orders",
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
    snapshot_dates = pd.to_datetime(df["snapshot_date"])
    df["week_start"] = (
        snapshot_dates - pd.to_timedelta(snapshot_dates.dt.weekday, unit="D")
    ).dt.date
    df["week_end"] = (pd.to_datetime(df["week_start"]) + pd.Timedelta(days=6)).dt.date
    df["report_week"] = (
        df["week_start"].astype(str) + " to " + df["week_end"].astype(str)
    )
    week_numbers = (
        pd.DataFrame({"week_start": sorted(df["week_start"].unique())})
        .assign(report_week_number=lambda weeks: range(1, len(weeks) + 1))
    )
    df = df.merge(week_numbers, on="week_start", how="left")
    df["report_week_label"] = "Week " + df["report_week_number"].astype(str)

    numeric_columns = [
        "total_orders",
        "overdue_open_orders",
        "late_delivery_rate",
        "avg_delay_days",
        "avg_review_score",
        "reviewed_orders",
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

    available_weeks = (
        df[["week_start", "week_end", "report_week", "report_week_label"]]
        .drop_duplicates()
        .sort_values("week_start")
    )
    week_starts = available_weeks["week_start"].tolist()
    week_label_by_start = dict(
        zip(
            available_weeks["week_start"],
            available_weeks["report_week_label"] + " (" + available_weeks["report_week"] + ")",
        )
    )

    selected_week_start = st.sidebar.selectbox(
        "report_week",
        options=week_starts,
        index=len(week_starts) - 1,
        format_func=lambda value: week_label_by_start[value],
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

    return selected_week_start, selected_risk_levels, min_total_orders


def get_week_end_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """Return the latest available daily snapshot inside each reporting week."""
    latest_dates = df.groupby("week_start", as_index=False)["snapshot_date"].max()
    return df.merge(latest_dates, on=["week_start", "snapshot_date"], how="inner")


def show_kpis(df: pd.DataFrame, reporting_days: int) -> None:
    """Show KPI cards for the selected reporting week."""
    unique_suppliers = df["seller_id"].nunique()
    high_risk_suppliers = len(df[df["risk_level"] == "high"])
    average_risk_score = df["risk_score"].mean() if not df.empty else 0
    overdue_open_orders = int(df["overdue_open_orders"].sum())

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Reporting days", f"{reporting_days:,}")
    col2.metric("Unique suppliers", f"{unique_suppliers:,}")
    col3.metric("High-risk suppliers", f"{high_risk_suppliers:,}")
    col4.metric("Average risk score", f"{average_risk_score:.3f}")
    col5.metric("Open overdue orders", f"{overdue_open_orders:,}")


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
    """Build weekly trend charts from week-end snapshots."""
    week_end_snapshots = get_week_end_snapshot(df)
    all_weeks = pd.DataFrame(
        {
            "week_start": sorted(week_end_snapshots["week_start"].unique())
        }
    )
    week_labels = (
        week_end_snapshots[["week_start", "report_week", "report_week_label"]]
        .drop_duplicates()
        .sort_values("week_start")
    )
    all_weeks = all_weeks.merge(week_labels, on="week_start", how="left")

    # Count high-risk suppliers per report week and keep zero-count weeks visible.
    high_risk_over_time = (
        week_end_snapshots[week_end_snapshots["risk_level"] == "high"]
        .groupby("week_start", as_index=False)
        .size()
        .rename(columns={"size": "high_risk_suppliers"})
    )
    high_risk_over_time = all_weeks.merge(
        high_risk_over_time,
        on="week_start",
        how="left",
    ).fillna({"high_risk_suppliers": 0})

    high_risk_chart = px.line(
        high_risk_over_time,
        x="report_week_label",
        y="high_risk_suppliers",
        markers=True,
        title="High-risk suppliers at week-end snapshot",
        hover_data={"report_week": True, "report_week_label": False},
        labels={
            "report_week_label": "Report week",
            "report_week": "Date range",
            "high_risk_suppliers": "High-risk suppliers",
        },
    )
    st.plotly_chart(high_risk_chart, use_container_width=True)

    average_risk_score_over_time = week_end_snapshots.groupby(
        ["week_start", "report_week", "report_week_label"],
        as_index=False,
    )[
        "risk_score"
    ].mean()
    average_risk_score_over_time = average_risk_score_over_time.sort_values("week_start")
    average_risk_score_chart = px.line(
        average_risk_score_over_time,
        x="report_week_label",
        y="risk_score",
        markers=True,
        title="Average risk score at week-end snapshot",
        hover_data={"report_week": True, "report_week_label": False},
        labels={
            "report_week_label": "Report week",
            "report_week": "Date range",
            "risk_score": "Average risk score",
        },
    )
    st.plotly_chart(average_risk_score_chart, use_container_width=True)

    delayed_order_value_over_time = week_end_snapshots.groupby(
        ["week_start", "report_week", "report_week_label"],
        as_index=False,
    )[
        "delayed_order_value"
    ].sum()
    delayed_order_value_over_time = delayed_order_value_over_time.sort_values("week_start")
    delayed_order_value_chart = px.line(
        delayed_order_value_over_time,
        x="report_week_label",
        y="delayed_order_value",
        markers=True,
        title="Delayed order value at week-end snapshot",
        hover_data={"report_week": True, "report_week_label": False},
        labels={
            "report_week_label": "Report week",
            "report_week": "Date range",
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
        "overdue_open_orders",
        "late_delivery_rate",
        "avg_delay_days",
        "avg_review_score",
        "reviewed_orders",
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
                "late_or_overdue_rate",
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
        page_title="Supplier Risk Weekly Report",
        layout="wide",
    )

    st.title("Supplier Risk Weekly Report")
    st.write(
        "This weekly report summarizes supplier risk from daily supplier risk snapshots. "
        "It highlights suppliers with repeated delivery delays, open overdue orders, "
        "low review scores, and high delayed order value."
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
        selected_week_start,
        selected_risk_levels,
        min_total_orders,
    ) = build_sidebar_filters(df)

    selected_week_df = df[df["week_start"] == selected_week_start]
    selected_week_end = selected_week_start + timedelta(days=6)
    report_snapshot_date = max(selected_week_df["snapshot_date"])
    report_snapshot_df = selected_week_df[
        selected_week_df["snapshot_date"] == report_snapshot_date
    ]
    current_risk_df = report_snapshot_df[
        report_snapshot_df["risk_level"].isin(selected_risk_levels)
    ]

    if current_risk_df.empty:
        st.warning("No supplier risk snapshots match the selected weekly filters.")
        st.stop()

    st.header("Weekly Risk Overview")
    st.caption(
        f"Report week: {selected_week_start} to {selected_week_end}. "
        f"Week-end snapshot: {report_snapshot_date}."
    )
    show_kpis(current_risk_df, selected_week_df["snapshot_date"].nunique())
    show_risk_distribution(current_risk_df)

    st.header("Weekly Risk Trends")
    show_trend_charts(df)

    st.header("Suppliers Requiring Attention")
    st.caption(
        "Showing high-risk suppliers on the selected report week's latest snapshot "
        f"with at least {min_total_orders} total orders."
    )
    attention_df = report_snapshot_df[
        (report_snapshot_df["risk_level"] == "high")
        & (report_snapshot_df["total_orders"] >= min_total_orders)
    ]
    show_suppliers_requiring_attention(attention_df)


if __name__ == "__main__":
    main()
