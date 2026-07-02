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
    "high": "#9f5f5f",
    "medium": "#c59a5b",
    "low": "#6f9f8b",
}
ACCENT_COLOR = "#6f7f95"
MUTED_COLOR = "#8a94a6"
DRIVER_COLORS = {
    "Delivery": "#9f5f5f",
    "Reviews": "#c59a5b",
    "Value exposure": "#6f7f95",
}
RISK_SCORE_GRADIENT = [
    [0.0, "#f3eee8"],
    [0.45, "#d9b98c"],
    [0.75, "#b9826f"],
    [1.0, "#8f4f5f"],
]


def inject_page_styles() -> None:
    """Add small CSS touches for a clearer operational dashboard."""
    st.markdown(
        """
        <style>
        div[data-testid="stMetric"] {
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            padding: 12px 14px;
            background: #ffffff;
        }
        .risk-callout {
            border-left: 6px solid #9f5f5f;
            border-radius: 8px;
            padding: 14px 16px;
            background: linear-gradient(90deg, #fbf3ef 0%, #ffffff 72%);
            border-top: 1px solid #ead4cb;
            border-right: 1px solid #ead4cb;
            border-bottom: 1px solid #ead4cb;
            margin: 6px 0 18px 0;
        }
        .risk-callout h3 {
            margin: 0 0 8px 0;
            color: #704141;
            font-size: 18px;
        }
        .risk-callout p {
            margin: 4px 0;
            color: #374151;
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
    available_week_ends = (
        df.groupby("week_start", as_index=False)["snapshot_date"]
        .max()
        .rename(columns={"snapshot_date": "available_week_end"})
    )
    df = df.merge(available_week_ends, on="week_start", how="left")
    df["report_week"] = (
        df["week_start"].astype(str) + " to " + df["available_week_end"].astype(str)
    )
    week_numbers = (
        pd.DataFrame({"week_start": sorted(df["week_start"].unique())})
        .assign(report_week_number=lambda weeks: range(1, len(weeks) + 1))
    )
    df = df.merge(week_numbers, on="week_start", how="left")
    df["report_week_label"] = "Week " + df["report_week_number"].astype(str)

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
        "risk_score",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    df["risk_level"] = df["risk_level"].fillna("unknown").str.lower()
    df = add_risk_explanations(df)

    return df


def format_currency(value: float) -> str:
    """Format a number as a simple currency-style value for Streamlit metrics."""
    return f"${value:,.2f}"


def add_risk_explanations(df: pd.DataFrame) -> pd.DataFrame:
    """Add readable driver labels that explain each supplier's risk score."""
    driver_columns = {
        "Delivery": df["late_delivery_rate"] * 0.4,
        "Reviews": df["low_review_rate"] * 0.3,
        "Value exposure": df["delayed_order_value_share"] * 0.3,
    }
    driver_scores = pd.DataFrame(driver_columns)
    df["primary_risk_driver"] = driver_scores.idxmax(axis=1)
    df["risk_reason"] = (
        "Delivery "
        + (df["late_delivery_rate"] * 100).round(0).astype(int).astype(str)
        + "% | Reviews "
        + (df["low_review_rate"] * 100).round(0).astype(int).astype(str)
        + "% | Value "
        + (df["delayed_order_value_share"] * 100).round(0).astype(int).astype(str)
        + "%"
    )
    return df


def polish_chart(fig) -> None:
    """Apply a calm, readable visual style to Plotly charts."""
    fig.update_layout(
        template="plotly_white",
        font={"family": "Arial", "size": 13, "color": "#1f2937"},
        title={"font": {"size": 18, "color": "#111827"}},
        legend_title_text="",
        margin={"l": 16, "r": 16, "t": 56, "b": 24},
        paper_bgcolor="white",
        plot_bgcolor="white",
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="#e5e7eb", zeroline=False)


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
        df[["week_start", "available_week_end", "report_week", "report_week_label"]]
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
    return df[df["snapshot_date"] == df["available_week_end"]]


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


def show_at_a_glance(df: pd.DataFrame, reporting_days: int) -> None:
    """Show the most important operational takeaways first."""
    high_risk_df = df[df["risk_level"] == "high"].copy()
    total_suppliers = df["seller_id"].nunique()
    high_risk_suppliers = high_risk_df["seller_id"].nunique()
    high_risk_share = (
        high_risk_suppliers / total_suppliers if total_suppliers else 0
    )

    top_supplier = None
    if not high_risk_df.empty:
        top_supplier = high_risk_df.sort_values(
            ["risk_score", "delayed_order_value"],
            ascending=[False, False],
        ).iloc[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("High-risk suppliers", f"{high_risk_suppliers:,}")
    col2.metric("High-risk share", f"{high_risk_share:.1%}")
    col3.metric("Open overdue orders", f"{int(df['overdue_open_orders'].sum()):,}")
    col4.metric("Delayed value", format_currency(df["delayed_order_value"].sum()))

    if top_supplier is None:
        st.success("No high-risk suppliers match the current filters.")
        return

    st.markdown(
        f"""
        <div class="risk-callout">
            <h3>Top priority supplier: {top_supplier["seller_id"]}</h3>
            <p><strong>Main driver:</strong> {top_supplier["primary_risk_driver"]} | <strong>Risk score:</strong> {top_supplier["risk_score"]:.3f}</p>
            <p><strong>Why:</strong> {top_supplier["risk_reason"]}</p>
            <p><strong>Operations:</strong> {int(top_supplier["total_orders"]):,} orders, {int(top_supplier["overdue_open_orders"]):,} open overdue, {top_supplier["avg_delivery_days"]:.1f} avg delivery days, {top_supplier["avg_delay_days"]:.1f} avg delay days, {format_currency(top_supplier["delayed_order_value"])} delayed value.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def show_risk_driver_distribution(df: pd.DataFrame, chart_key: str) -> None:
    """Show which signal is driving the high-risk supplier group."""
    high_risk_df = df[df["risk_level"] == "high"].copy()
    if high_risk_df.empty:
        st.info("No high-risk suppliers match the current filters.")
        return

    driver_counts = (
        high_risk_df.groupby("primary_risk_driver", as_index=False)
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
        title="High-risk suppliers by main driver",
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
    )
    driver_chart = px.bar(
        driver_df,
        x="risk_score",
        y="seller_id",
        orientation="h",
        color="risk_score",
        title="Why top high-risk suppliers are flagged",
        color_continuous_scale=RISK_SCORE_GRADIENT,
        hover_data={
            "seller_id": False,
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
            "seller_id": "Supplier",
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
        st.info("No high-risk suppliers match the current driver filters.")
        return

    breakdown_df = (
        df.sort_values("risk_score", ascending=False)
        .head(10)
        .loc[
            :,
            [
                "seller_id",
                "late_delivery_rate",
                "low_review_rate",
                "delayed_order_value_share",
            ],
        ]
        .copy()
    )
    breakdown_df["Delivery"] = breakdown_df["late_delivery_rate"] * 0.4
    breakdown_df["Reviews"] = breakdown_df["low_review_rate"] * 0.3
    breakdown_df["Value exposure"] = (
        breakdown_df["delayed_order_value_share"] * 0.3
    )
    breakdown_long = breakdown_df.melt(
        id_vars="seller_id",
        value_vars=["Delivery", "Reviews", "Value exposure"],
        var_name="risk_component",
        value_name="score_contribution",
    )

    breakdown_chart = px.bar(
        breakdown_long,
        x="score_contribution",
        y="seller_id",
        color="risk_component",
        orientation="h",
        title="Risk score breakdown for top high-risk suppliers",
        color_discrete_map=DRIVER_COLORS,
        labels={
            "score_contribution": "Weighted score contribution",
            "seller_id": "Supplier",
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
    """Show high-risk suppliers that need business attention."""
    table_columns = [
        "snapshot_date",
        "seller_id",
        "total_orders",
        "late_orders",
        "overdue_open_orders",
        "primary_risk_driver",
        "risk_reason",
        "late_delivery_rate",
        "low_review_rate",
        "delayed_order_value_share",
        "avg_delivery_days",
        "avg_open_order_age_days",
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

    selected_week_df = filtered_df[filtered_df["week_start"] == selected_week_start]
    if selected_week_df.empty:
        st.warning("No supplier risk snapshots match the selected weekly filters.")
        st.stop()

    selected_week_end = max(selected_week_df["available_week_end"])
    report_snapshot_date = max(selected_week_df["snapshot_date"])
    report_snapshot_df = selected_week_df[
        selected_week_df["snapshot_date"] == report_snapshot_date
    ]

    if report_snapshot_df.empty:
        st.warning("No supplier risk snapshots match the selected weekly filters.")
        st.stop()

    attention_df = report_snapshot_df[
        report_snapshot_df["risk_level"] == "high"
    ]

    st.caption(
        f"Report week: {selected_week_start} to {selected_week_end}. "
        f"Snapshot used: {report_snapshot_date}. "
        f"Minimum total orders: {min_total_orders}."
    )

    summary_tab, drivers_tab, trends_tab, detail_tab = st.tabs(
        [
            "Executive Summary",
            "Risk Drivers",
            "Weekly Trends",
            "Supplier Detail",
        ]
    )

    with summary_tab:
        show_at_a_glance(
            report_snapshot_df,
            selected_week_df["snapshot_date"].nunique(),
        )
        show_kpis(report_snapshot_df, selected_week_df["snapshot_date"].nunique())
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
            )

    with drivers_tab:
        driver_col1, driver_col2 = st.columns([1, 1.2])
        with driver_col1:
            show_risk_driver_distribution(
                report_snapshot_df,
                chart_key="drivers_risk_driver_distribution",
            )
        with driver_col2:
            show_high_risk_driver_chart(attention_df)
        show_risk_score_breakdown(attention_df)

    with trends_tab:
        show_trend_charts(filtered_df)

    with detail_tab:
        show_suppliers_requiring_attention(attention_df)


if __name__ == "__main__":
    main()
