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
    "high": "#b65b5b",
    "medium": "#d99a2b",
    "low": "#4f9d7a",
}
ACCENT_COLOR = "#4d6fa3"
MUTED_COLOR = "#667085"
DRIVER_COLORS = {
    "Delivery": "#b65b5b",
    "Reviews": "#d99a2b",
    "Value exposure": "#4d6fa3",
}
RISK_SCORE_GRADIENT = [
    [0.0, "#f2f7f4"],
    [0.45, "#f6d99d"],
    [0.75, "#dc8a73"],
    [1.0, "#a94747"],
]


def inject_page_styles() -> None:
    """Add small CSS touches for a clearer operational dashboard."""
    st.markdown(
        """
        <style>
        div[data-testid="stMetric"] {
            border: 1px solid #d0d5dd;
            border-radius: 8px;
            padding: 12px 14px;
            background: #fcfcfd;
        }
        div[data-testid="stMetric"] label {
            color: #475467;
            font-size: 13px;
        }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: #101828;
            font-weight: 600;
        }
        section[data-testid="stSidebar"] {
            background-color: #f8fafc;
        }
        .risk-callout {
            border-left: 6px solid #b65b5b;
            border-radius: 8px;
            padding: 14px 16px;
            background: linear-gradient(90deg, #fff4f2 0%, #ffffff 78%);
            border-top: 1px solid #f1c9c3;
            border-right: 1px solid #f1c9c3;
            border-bottom: 1px solid #f1c9c3;
            margin: 6px 0 18px 0;
        }
        .risk-callout h3 {
            margin: 0 0 8px 0;
            color: #7a2e2e;
            font-size: 18px;
        }
        .risk-callout p {
            margin: 4px 0;
            color: #344054;
            font-size: 14px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


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

    return snowflake.connector.connect(**connection_args)


@st.cache_data(ttl=300)
def load_supplier_risk_data() -> pd.DataFrame:
    """Load the final dbt mart into a pandas DataFrame."""
    query = f"""
        select
            report_week_start::date as "report_week_start",
            report_week_end::date as "report_week_end",
            reporting_week_snapshot_days as "reporting_week_snapshot_days",
            is_complete_reporting_week as "is_complete_reporting_week",
            snapshot_date::date as "snapshot_date",
            seller_id as "seller_id",
            total_orders as "total_orders",
            late_orders as "late_orders",
            overdue_open_orders as "overdue_open_orders",
            late_delivery_rate as "late_delivery_rate",
            avg_delay_days as "avg_delay_days",
            avg_delivery_days as "avg_delivery_days",
            avg_open_order_age_days as "avg_open_order_age_days",
            avg_review_score as "avg_review_score",
            reviewed_orders as "reviewed_orders",
            low_review_count as "low_review_count",
            low_review_rate as "low_review_rate",
            total_order_value as "total_order_value",
            delayed_order_value as "delayed_order_value",
            delayed_order_value_share as "delayed_order_value_share",
            risk_score_delivery_component as "risk_score_delivery_component",
            risk_score_review_component as "risk_score_review_component",
            risk_score_value_component as "risk_score_value_component",
            risk_score as "risk_score",
            primary_risk_driver as "primary_risk_driver",
            risk_reason as "risk_reason",
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
    df["week_start"] = pd.to_datetime(df["report_week_start"]).dt.date
    df["week_end"] = pd.to_datetime(df["report_week_end"]).dt.date
    df["report_week"] = (
        df["week_start"].astype(str) + " to " + df["week_end"].astype(str)
    )
    df["snapshot_days"] = pd.to_numeric(
        df["reporting_week_snapshot_days"],
        errors="coerce",
    ).fillna(0)
    df["is_complete_week"] = df["is_complete_reporting_week"].fillna(False).astype(bool)
    df["report_week_label"] = df["report_week"]

    numeric_columns = [
        "total_orders",
        "late_orders",
        "overdue_open_orders",
        "late_delivery_rate",
        "avg_delay_days",
        "avg_delivery_days",
        "avg_open_order_age_days",
        "avg_review_score",
        "reviewed_orders",
        "low_review_count",
        "low_review_rate",
        "total_order_value",
        "delayed_order_value",
        "delayed_order_value_share",
        "risk_score_delivery_component",
        "risk_score_review_component",
        "risk_score_value_component",
        "risk_score",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    df["risk_level"] = df["risk_level"].fillna("unknown").str.lower()
    df["primary_risk_driver"] = df["primary_risk_driver"].fillna("Unknown")
    df["risk_reason"] = df["risk_reason"].fillna("No risk driver available")

    return df


def format_currency(value: float) -> str:
    """Format a number as a simple currency-style value for Streamlit metrics."""
    return f"${value:,.2f}"


def shorten_identifier(value: str, prefix: int = 8, suffix: int = 4) -> str:
    """Keep long synthetic IDs readable in charts and callouts."""
    text = str(value)
    if len(text) <= prefix + suffix + 3:
        return text
    return f"{text[:prefix]}...{text[-suffix:]}"


def polish_chart(fig) -> None:
    """Apply a calm, readable visual style to Plotly charts."""
    fig.update_layout(
        template="plotly_white",
        font={"family": "Arial", "size": 12, "color": "#344054"},
        title={"font": {"size": 16, "color": "#101828"}},
        legend_title_text="",
        margin={"l": 16, "r": 16, "t": 56, "b": 24},
        paper_bgcolor="white",
        plot_bgcolor="white",
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="#eaecf0", zeroline=False)


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
        df.loc[
            df["is_complete_week"],
            ["week_start", "week_end", "report_week", "report_week_label"],
        ]
        .drop_duplicates()
        .sort_values("week_start")
    )
    week_starts = available_weeks["week_start"].tolist()
    week_label_by_start = dict(
        zip(
            available_weeks["week_start"],
            available_weeks["report_week_label"],
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


def apply_sidebar_filters(
    df: pd.DataFrame,
    selected_risk_levels: list[str],
    min_total_orders: int,
) -> pd.DataFrame:
    """Apply supplier filters consistently across KPIs, charts, and tables."""
    return df[
        df["risk_level"].isin(selected_risk_levels)
        & (df["total_orders"] >= min_total_orders)
    ].copy()


def get_week_end_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """Return the latest available daily snapshot inside each reporting week."""
    return df[df["snapshot_date"] == df["week_end"]]


def show_at_a_glance(df: pd.DataFrame, reporting_days: int) -> None:
    """Show the most important operational takeaways first."""
    high_risk_df = df[df["risk_level"] == "high"].copy()
    medium_risk_df = df[df["risk_level"] == "medium"].copy()
    eligible_suppliers = df["seller_id"].nunique()
    attention_suppliers = df[df["risk_level"].isin(["high", "medium"])][
        "seller_id"
    ].nunique()
    high_risk_suppliers = high_risk_df["seller_id"].nunique()
    medium_risk_suppliers = medium_risk_df["seller_id"].nunique()

    top_supplier = None
    if not high_risk_df.empty:
        top_supplier = high_risk_df.sort_values(
            ["risk_score", "delayed_order_value"],
            ascending=[False, False],
        ).iloc[0]

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Reporting days", f"{reporting_days:,}")
    col2.metric("Eligible suppliers", f"{eligible_suppliers:,}")
    col3.metric("Attention suppliers", f"{attention_suppliers:,}")
    col4.metric("High-risk suppliers", f"{high_risk_suppliers:,}")
    col5.metric(
        "Week-end overdue orders", f"{int(df['overdue_open_orders'].sum()):,}"
    )
    col6.metric(
        "Week-end delayed value", format_currency(df["delayed_order_value"].sum())
    )

    if top_supplier is None:
        if medium_risk_suppliers:
            st.success(
                "No urgent high-risk suppliers this week. "
                "Medium-risk suppliers are still monitored as attention cases."
            )
        else:
            st.success("No high- or medium-risk suppliers match the current filters.")
        return

    top_supplier_label = shorten_identifier(top_supplier["seller_id"])
    st.markdown(
        f"""
        <div class="risk-callout">
            <h3>Top priority supplier: {top_supplier_label}</h3>
            <p><strong>Main driver:</strong> {top_supplier["primary_risk_driver"]} | <strong>Risk score:</strong> {top_supplier["risk_score"]:.3f}</p>
            <p><strong>Why:</strong> {top_supplier["risk_reason"]}</p>
            <p><strong>Operations:</strong> {int(top_supplier["total_orders"]):,} orders, {int(top_supplier["overdue_open_orders"]):,} open overdue, {top_supplier["avg_delivery_days"]:.1f} avg delivery days, {top_supplier["avg_delay_days"]:.1f} avg delay days, {format_currency(top_supplier["delayed_order_value"])} delayed value.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def show_risk_driver_distribution(
    df: pd.DataFrame,
    chart_key: str,
    title: str,
) -> None:
    """Show which signal is driving the selected supplier group."""
    if df.empty:
        st.info("No suppliers match the current driver filters.")
        return

    driver_counts = (
        df.groupby("primary_risk_driver", as_index=False)
        .size()
        .rename(columns={"size": "suppliers"})
        .sort_values("suppliers", ascending=False)
    )
    driver_chart = px.bar(
        driver_counts,
        x="suppliers",
        y="primary_risk_driver",
        orientation="h",
        color="primary_risk_driver",
        title=title,
        color_discrete_map=DRIVER_COLORS,
        labels={
            "suppliers": "Suppliers",
            "primary_risk_driver": "Main driver",
        },
    )
    polish_chart(driver_chart)
    driver_chart.update_layout(height=260)
    st.plotly_chart(driver_chart, use_container_width=True, key=chart_key)


def show_risk_distribution(df: pd.DataFrame, chart_key: str) -> None:
    """Show the risk level mix for the selected snapshot date."""
    risk_distribution = (
        df.groupby("risk_level", as_index=False)
        .size()
        .rename(columns={"size": "supplier_snapshots"})
    )
    risk_distribution = (
        risk_distribution.set_index("risk_level")
        .reindex(RISK_LEVEL_ORDER, fill_value=0)
        .reset_index()
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
    polish_chart(risk_distribution_chart)
    st.plotly_chart(risk_distribution_chart, use_container_width=True, key=chart_key)


def show_trend_charts(df: pd.DataFrame) -> None:
    """Build weekly trend charts from week-end snapshots."""
    complete_week_df = df[df["is_complete_week"]].copy()
    week_end_snapshots = get_week_end_snapshot(complete_week_df)
    if week_end_snapshots.empty:
        st.info("No complete Monday-to-Sunday reporting weeks match the trend filters.")
        return

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

    overdue_orders_over_time = week_end_snapshots.groupby(
        ["week_start", "report_week", "report_week_label"],
        as_index=False,
    )["overdue_open_orders"].sum()
    overdue_orders_over_time = all_weeks.merge(
        overdue_orders_over_time,
        on=["week_start", "report_week", "report_week_label"],
        how="left",
    ).fillna({"overdue_open_orders": 0})

    overdue_orders_chart = px.line(
        overdue_orders_over_time,
        x="report_week_label",
        y="overdue_open_orders",
        markers=True,
        title="Open overdue orders at week-end snapshot",
        hover_data={"report_week": True, "report_week_label": False},
        labels={
            "report_week_label": "Report week",
            "report_week": "Date range",
            "overdue_open_orders": "Open overdue orders",
        },
    )
    overdue_orders_chart.update_traces(line_color=RISK_LEVEL_COLORS["high"])
    polish_chart(overdue_orders_chart)

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
    average_risk_score_chart.update_traces(line_color=ACCENT_COLOR)
    polish_chart(average_risk_score_chart)

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
    delayed_order_value_chart.update_traces(line_color=MUTED_COLOR)
    polish_chart(delayed_order_value_chart)

    trend_col1, trend_col2, trend_col3 = st.columns(3)
    with trend_col1:
        st.plotly_chart(
            overdue_orders_chart,
            use_container_width=True,
            key="trend_overdue_orders",
        )
    with trend_col2:
        st.plotly_chart(
            average_risk_score_chart,
            use_container_width=True,
            key="trend_average_risk_score",
        )
    with trend_col3:
        st.plotly_chart(
            delayed_order_value_chart,
            use_container_width=True,
            key="trend_delayed_order_value",
        )


def show_high_risk_driver_chart(df: pd.DataFrame) -> None:
    """Show why the highest-risk suppliers are being flagged."""
    if df.empty:
        return

    driver_df = (
        df.sort_values("risk_score", ascending=False)
        .head(12)
        .sort_values("risk_score")
        .assign(supplier_label=lambda frame: frame["seller_id"].map(shorten_identifier))
    )
    driver_chart = px.bar(
        driver_df,
        x="risk_score",
        y="supplier_label",
        orientation="h",
        color="risk_score",
        title="Highest-risk suppliers requiring attention",
        color_continuous_scale=RISK_SCORE_GRADIENT,
        hover_data={
            "seller_id": True,
            "supplier_label": False,
            "primary_risk_driver": True,
            "total_orders": True,
            "late_delivery_rate": ":.2f",
            "avg_delivery_days": ":.1f",
            "avg_delay_days": ":.1f",
            "avg_review_score": ":.2f",
            "delayed_order_value": ":,.2f",
        },
        labels={
            "risk_score": "Risk score",
            "supplier_label": "Supplier",
            "seller_id": "Full supplier ID",
            "primary_risk_driver": "Main driver",
            "total_orders": "Orders",
            "late_delivery_rate": "Late/overdue rate",
            "avg_delivery_days": "Avg delivery days",
            "avg_delay_days": "Avg delay days",
            "avg_review_score": "Avg review score",
            "delayed_order_value": "Delayed order value",
        },
    )
    polish_chart(driver_chart)
    driver_chart.update_layout(coloraxis_showscale=False)
    st.plotly_chart(
        driver_chart,
        use_container_width=True,
        key="top_high_risk_driver_chart",
    )


def show_risk_score_breakdown(df: pd.DataFrame) -> None:
    """Show the weighted score components for the highest-risk suppliers."""
    if df.empty:
        st.info("No attention suppliers match the current driver filters.")
        return

    breakdown_df = (
        df.sort_values("risk_score", ascending=False)
        .head(10)
        .loc[
            :,
            [
                "seller_id",
                "risk_score_delivery_component",
                "risk_score_review_component",
                "risk_score_value_component",
            ],
        ]
        .copy()
    )
    breakdown_df["supplier_label"] = breakdown_df["seller_id"].map(shorten_identifier)
    breakdown_df = breakdown_df.rename(
        columns={
            "risk_score_delivery_component": "Delivery",
            "risk_score_review_component": "Reviews",
            "risk_score_value_component": "Value exposure",
        }
    )
    breakdown_long = breakdown_df.melt(
        id_vars=["supplier_label", "seller_id"],
        value_vars=["Delivery", "Reviews", "Value exposure"],
        var_name="risk_component",
        value_name="score_contribution",
    )

    breakdown_chart = px.bar(
        breakdown_long,
        x="score_contribution",
        y="supplier_label",
        color="risk_component",
        orientation="h",
        title="Risk score breakdown for attention suppliers",
        color_discrete_map=DRIVER_COLORS,
        hover_data={"seller_id": True, "supplier_label": False},
        labels={
            "score_contribution": "Weighted score contribution",
            "supplier_label": "Supplier",
            "seller_id": "Full supplier ID",
            "risk_component": "Risk component",
        },
    )
    polish_chart(breakdown_chart)
    breakdown_chart.update_layout(barmode="stack")
    st.plotly_chart(
        breakdown_chart,
        use_container_width=True,
        key="risk_score_breakdown",
    )


def show_suppliers_requiring_attention(df: pd.DataFrame) -> None:
    """Show high- and medium-risk suppliers that need business attention."""
    table_columns = [
        "snapshot_date",
        "supplier",
        "seller_id",
        "risk_level",
        "risk_score",
        "primary_risk_driver",
        "risk_reason",
        "total_orders",
        "late_orders",
        "overdue_open_orders",
        "delayed_order_value",
        "late_delivery_rate",
        "low_review_rate",
        "delayed_order_value_share",
        "avg_delivery_days",
        "avg_open_order_age_days",
        "avg_delay_days",
        "avg_review_score",
        "reviewed_orders",
    ]

    if df.empty:
        st.info("No attention suppliers match the current filters.")
        return

    attention_table = (
        df.assign(supplier=lambda frame: frame["seller_id"].map(shorten_identifier))[table_columns]
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
            "supplier": st.column_config.TextColumn("supplier"),
            "seller_id": st.column_config.TextColumn("full_seller_id"),
            "risk_level": st.column_config.TextColumn("risk_level"),
            "late_delivery_rate": st.column_config.NumberColumn(
                "late_or_overdue_rate",
                format="%.2f",
            ),
            "low_review_rate": st.column_config.NumberColumn(
                "low_review_rate",
                format="%.2f",
            ),
            "delayed_order_value_share": st.column_config.NumberColumn(
                "delayed_value_share",
                format="%.2f",
            ),
            "avg_delivery_days": st.column_config.NumberColumn(
                "avg_delivery_days",
                format="%.1f",
            ),
            "avg_open_order_age_days": st.column_config.NumberColumn(
                "avg_open_order_age_days",
                format="%.1f",
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
    inject_page_styles()

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

    if not df["is_complete_week"].any():
        st.warning(
            "No complete Monday-to-Sunday reporting weeks are available yet. "
            "Load at least one full calendar week of daily snapshots to show the weekly report."
        )
        st.stop()

    (
        selected_week_start,
        selected_risk_levels,
        min_total_orders,
    ) = build_sidebar_filters(df)

    filtered_df = apply_sidebar_filters(
        df=df,
        selected_risk_levels=selected_risk_levels,
        min_total_orders=min_total_orders,
    )

    selected_week_df = filtered_df[
        (filtered_df["week_start"] == selected_week_start)
        & (filtered_df["is_complete_week"])
    ]
    if selected_week_df.empty:
        st.warning("No supplier risk snapshots match the selected weekly filters.")
        st.stop()

    selected_week_end = max(selected_week_df["week_end"])
    report_snapshot_date = max(selected_week_df["snapshot_date"])
    report_snapshot_df = selected_week_df[
        selected_week_df["snapshot_date"] == report_snapshot_date
    ]

    if report_snapshot_df.empty:
        st.warning("No supplier risk snapshots match the selected weekly filters.")
        st.stop()

    attention_df = report_snapshot_df[
        report_snapshot_df["risk_level"].isin(["high", "medium"])
    ].copy()

    st.caption(
        f"Report week: {selected_week_start} to {selected_week_end}. "
        f"Snapshot used: {report_snapshot_date}. "
        f"Minimum total orders: {min_total_orders}."
    )

    summary_tab, drivers_tab, trends_tab, attention_tab = st.tabs(
        [
            "Executive Summary",
            "Risk Drivers",
            "Weekly Trends",
            "Suppliers Requiring Attention",
        ]
    )

    with summary_tab:
        show_at_a_glance(
            report_snapshot_df,
            selected_week_df["snapshot_date"].nunique(),
        )
        overview_col1, overview_col2 = st.columns(2)
        with overview_col1:
            show_risk_distribution(
                report_snapshot_df,
                chart_key="summary_risk_distribution",
            )
        with overview_col2:
            show_risk_driver_distribution(
                report_snapshot_df,
                chart_key="summary_risk_driver_distribution",
                title="Suppliers by main risk driver",
            )

    with drivers_tab:
        driver_col1, driver_col2 = st.columns([1, 1.2])
        with driver_col1:
            show_risk_driver_distribution(
                attention_df,
                chart_key="drivers_risk_driver_distribution",
                title="Attention suppliers by main driver",
            )
        with driver_col2:
            show_high_risk_driver_chart(attention_df)
        show_risk_score_breakdown(attention_df)

    with trends_tab:
        st.caption(
            "Weekly trends use the selected sidebar filters and compare week-end snapshots only."
        )
        show_trend_charts(filtered_df)

    with attention_tab:
        show_suppliers_requiring_attention(attention_df)


if __name__ == "__main__":
    main()
