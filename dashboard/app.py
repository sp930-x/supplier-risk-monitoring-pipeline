import os
from datetime import date

import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv


MART_TABLE = "SUPPLIER_RISK.ANALYTICS.MART_SUPPLIER_RISK_SNAPSHOT"
RISK_LEVEL_ORDER = ["high", "medium", "low"]
MEDIUM_RISK_THRESHOLD = 0.40
HIGH_RISK_THRESHOLD = 0.70
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
            font-size: 14px;
        }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: #101828;
            font-weight: 600;
        }
        div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
            font-size: 13px;
        }
        section[data-testid="stSidebar"] {
            background-color: #f8fafc;
        }
        section[data-testid="stSidebar"] h2 {
            font-size: 18px;
        }
        section[data-testid="stSidebar"] label p,
        section[data-testid="stSidebar"] div[data-testid="stWidgetLabel"] p {
            font-size: 14px;
        }
        section[data-testid="stSidebar"] div[data-baseweb="select"] span,
        section[data-testid="stSidebar"] div[data-baseweb="tag"] span {
            font-size: 14px;
        }
        div[data-testid="stCaptionContainer"] p {
            font-size: 13px;
        }
        div[data-testid="stAlert"] p {
            font-size: 15px;
        }
        div[data-testid="stDataFrame"] {
            font-size: 14px;
        }
        div[data-testid="stTabs"] button[data-baseweb="tab"] {
            padding: 11px 16px;
        }
        div[data-testid="stTabs"] button[data-baseweb="tab"] p {
            color: #344054;
            font-size: 15px;
            font-weight: 500;
        }
        div[data-testid="stTabs"] button[aria-selected="true"] p {
            color: #e5484d;
            font-weight: 700;
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
            font-size: 20px;
        }
        .risk-callout p {
            margin: 4px 0;
            color: #344054;
            font-size: 15px;
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


def get_supplier_id_column(df: pd.DataFrame) -> str:
    """Find the supplier ID column used by the current dataframe."""
    for column in ["seller_id", "full_seller_id", "supplier"]:
        if column in df.columns:
            return column
    raise KeyError("No supplier ID column found for supplier risk charts.")


def add_seller_short_label(df: pd.DataFrame) -> pd.DataFrame:
    """Add a short supplier label while keeping the full supplier ID available."""
    supplier_id_column = get_supplier_id_column(df)
    labeled_df = df.copy()
    labeled_df["seller_short"] = labeled_df[supplier_id_column].map(
        lambda value: shorten_identifier(value, prefix=6, suffix=4)
    )
    return labeled_df


def get_available_columns(df: pd.DataFrame, columns: list[str]) -> list[str]:
    """Keep table selections resilient when an optional column is unavailable."""
    return [column for column in columns if column in df.columns]


def safe_sum(df: pd.DataFrame, column: str) -> float:
    """Return a numeric sum without failing on missing columns."""
    if column not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())


def safe_mean(df: pd.DataFrame, column: str) -> float:
    """Return a numeric mean without failing on missing columns or empty frames."""
    if column not in df.columns or df.empty:
        return 0.0
    values = pd.to_numeric(df[column], errors="coerce").dropna()
    if values.empty:
        return 0.0
    return float(values.mean())


def summarize_snapshot(df: pd.DataFrame) -> dict[str, float]:
    """Summarize the KPIs shown in the executive header."""
    if df.empty:
        return {
            "eligible_suppliers": 0,
            "attention_suppliers": 0,
            "high_risk_suppliers": 0,
            "medium_risk_suppliers": 0,
            "overdue_orders": 0,
            "delayed_value": 0.0,
            "average_risk_score": 0.0,
        }

    return {
        "eligible_suppliers": df["seller_id"].nunique() if "seller_id" in df.columns else len(df),
        "attention_suppliers": df[df["risk_level"].isin(["high", "medium"])]["seller_id"].nunique()
        if {"risk_level", "seller_id"}.issubset(df.columns)
        else 0,
        "high_risk_suppliers": df[df["risk_level"] == "high"]["seller_id"].nunique()
        if {"risk_level", "seller_id"}.issubset(df.columns)
        else 0,
        "medium_risk_suppliers": df[df["risk_level"] == "medium"]["seller_id"].nunique()
        if {"risk_level", "seller_id"}.issubset(df.columns)
        else 0,
        "overdue_orders": safe_sum(df, "overdue_open_orders"),
        "delayed_value": safe_sum(df, "delayed_order_value"),
        "average_risk_score": safe_mean(df, "risk_score"),
    }


def get_report_snapshot(df: pd.DataFrame, week_start: date) -> pd.DataFrame:
    """Return the latest snapshot for one complete report week."""
    week_df = df[(df["week_start"] == week_start) & (df["is_complete_week"])].copy()
    if week_df.empty:
        return week_df
    report_snapshot_date = max(week_df["snapshot_date"])
    return week_df[week_df["snapshot_date"] == report_snapshot_date].copy()


def get_previous_report_snapshot(
    df: pd.DataFrame,
    selected_week_start: date,
) -> pd.DataFrame | None:
    """Find the previous complete report week after the current filters are applied."""
    previous_weeks = sorted(
        week_start
        for week_start in df.loc[df["is_complete_week"], "week_start"].dropna().unique()
        if week_start < selected_week_start
    )
    if not previous_weeks:
        return None
    previous_snapshot = get_report_snapshot(df, previous_weeks[-1])
    if previous_snapshot.empty:
        return None
    return previous_snapshot


def summarize_weekly_trends(week_end_snapshots: pd.DataFrame) -> pd.DataFrame:
    """Aggregate week-end snapshots into the three trend metrics."""
    if week_end_snapshots.empty:
        return pd.DataFrame(
            columns=[
                "week_start",
                "report_week",
                "report_week_label",
                "overdue_open_orders",
                "risk_score",
                "delayed_order_value",
            ]
        )

    return (
        week_end_snapshots.groupby(
            ["week_start", "report_week", "report_week_label"],
            as_index=False,
        )
        .agg(
            overdue_open_orders=("overdue_open_orders", "sum"),
            risk_score=("risk_score", "mean"),
            delayed_order_value=("delayed_order_value", "sum"),
        )
        .sort_values("week_start")
    )


def build_trend_interpretation(
    weekly_trends: pd.DataFrame,
    selected_week_start: date,
) -> str:
    """Create concise interpretation text for the trend tab."""
    selected_rows = weekly_trends[weekly_trends["week_start"] == selected_week_start]
    previous_rows = weekly_trends[weekly_trends["week_start"] < selected_week_start]

    if selected_rows.empty:
        return "No complete week-end trend data is available for the selected week."

    if previous_rows.empty:
        return "No previous week comparison available for the selected trend view."

    current = selected_rows.iloc[-1]
    previous = previous_rows.sort_values("week_start").iloc[-1]
    return " ".join(
        [
            describe_change(
                "Open overdue orders",
                float(current["overdue_open_orders"]),
                float(previous["overdue_open_orders"]),
            ),
            describe_change(
                "Average risk score",
                float(current["risk_score"]),
                float(previous["risk_score"]),
                value_type="score",
            ),
            describe_change(
                "Delayed order value",
                float(current["delayed_order_value"]),
                float(previous["delayed_order_value"]),
                value_type="currency",
            ),
        ]
    )


def format_delta(
    current_value: float,
    previous_value: float | None,
    *,
    value_type: str = "number",
    include_percent: bool = True,
) -> str:
    """Format a concise week-over-week KPI delta."""
    if previous_value is None:
        return "No previous week comparison available"

    change = current_value - previous_value
    sign = "+" if change > 0 else ""

    if value_type == "currency":
        absolute_change = f"{sign}{format_currency(change)}"
    elif value_type == "score":
        absolute_change = f"{sign}{change:.3f}"
    else:
        absolute_change = f"{sign}{int(round(change)):,}"

    if not include_percent or previous_value == 0:
        if value_type == "score":
            return f"{absolute_change} score points vs previous week"
        return f"{absolute_change} vs previous week"

    percent_change = change / previous_value
    percent_sign = "+" if percent_change > 0 else ""
    return (
        f"{absolute_change} ({percent_sign}{percent_change:.0%}) "
        "vs previous week"
    )


def describe_change(
    metric_name: str,
    current_value: float,
    previous_value: float | None,
    *,
    value_type: str = "number",
) -> str:
    """Describe a metric movement for executive insight text."""
    if previous_value is None:
        return f"{metric_name} has no previous week comparison."

    change = current_value - previous_value
    if abs(change) < 0.0001:
        return f"{metric_name} was unchanged versus the previous week."

    direction = "increased" if change > 0 else "decreased"
    if value_type == "currency":
        change_text = format_currency(abs(change))
    elif value_type == "score":
        change_text = f"{abs(change):.3f}"
    else:
        change_text = f"{int(round(abs(change))):,}"
    return f"{metric_name} {direction} by {change_text} versus the previous week."


def build_executive_insight(
    current_summary: dict[str, float],
    previous_summary: dict[str, float] | None,
) -> str:
    """Create a short business-oriented weekly summary."""
    sentences = []
    high_risk_suppliers = int(current_summary["high_risk_suppliers"])
    attention_suppliers = int(current_summary["attention_suppliers"])
    medium_risk_suppliers = int(current_summary["medium_risk_suppliers"])

    if high_risk_suppliers == 0:
        sentences.append("No high-risk suppliers this week.")
    else:
        sentences.append(f"{high_risk_suppliers:,} high-risk suppliers need priority review.")

    if attention_suppliers > 0:
        sentences.append(
            f"{medium_risk_suppliers:,} medium-risk suppliers still require monitoring."
        )

    if previous_summary is None:
        sentences.append("No previous week comparison available.")
    else:
        overdue_change = current_summary["overdue_orders"] - previous_summary["overdue_orders"]
        delayed_value_change = (
            current_summary["delayed_value"] - previous_summary["delayed_value"]
        )
        if overdue_change < 0 and delayed_value_change > 0:
            sentences.append(
                "Although overdue orders decreased by "
                f"{int(round(abs(overdue_change))):,} versus the previous week, "
                f"delayed order value increased by {format_currency(delayed_value_change)}, "
                "indicating continued financial exposure."
            )
            return " ".join(sentences)

        spike_drivers = []
        previous_overdue = previous_summary["overdue_orders"]
        previous_delayed_value = previous_summary["delayed_value"]
        if (
            previous_overdue > 0
            and current_summary["overdue_orders"] > previous_overdue * 2
        ):
            spike_drivers.append("overdue orders")
        if (
            previous_delayed_value > 0
            and current_summary["delayed_value"] > previous_delayed_value * 2
        ):
            spike_drivers.append("delayed order value exposure")
        if spike_drivers:
            sentences.append(
                "This week shows a significant operational risk spike compared "
                f"with the previous week, mainly driven by {' and '.join(spike_drivers)}."
            )
        sentences.append(
            describe_change(
                "Overdue orders",
                current_summary["overdue_orders"],
                previous_summary["overdue_orders"],
            )
        )
        sentences.append(
            describe_change(
                "Delayed order value",
                current_summary["delayed_value"],
                previous_summary["delayed_value"],
                value_type="currency",
            )
        )

    return " ".join(sentences)


def polish_chart(fig) -> None:
    """Apply a calm, readable visual style to Plotly charts."""
    fig.update_layout(
        template="plotly_white",
        font={"family": "Arial", "size": 14, "color": "#344054"},
        title={"font": {"size": 20, "color": "#101828"}},
        legend_title_text="",
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": -0.18,
            "xanchor": "center",
            "x": 0.5,
            "font": {"size": 13},
        },
        margin={"l": 16, "r": 16, "t": 60, "b": 68},
        paper_bgcolor="white",
        plot_bgcolor="white",
    )
    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        title_font={"size": 14},
        tickfont={"size": 13},
    )
    fig.update_yaxes(
        gridcolor="#eaecf0",
        zeroline=False,
        title_font={"size": 14},
        tickfont={"size": 13},
    )


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


def show_at_a_glance(
    df: pd.DataFrame,
    reporting_days: int,
    previous_df: pd.DataFrame | None = None,
) -> None:
    """Show the most important operational takeaways first."""
    current_summary = summarize_snapshot(df)
    previous_summary = summarize_snapshot(previous_df) if previous_df is not None else None
    high_risk_df = df[df["risk_level"] == "high"].copy()
    medium_risk_df = df[df["risk_level"] == "medium"].copy()

    top_supplier = None
    if not high_risk_df.empty:
        top_supplier = high_risk_df.sort_values(
            ["risk_score", "delayed_order_value"],
            ascending=[False, False],
        ).iloc[0]

    previous_attention = (
        previous_summary["attention_suppliers"] if previous_summary is not None else None
    )
    previous_high = (
        previous_summary["high_risk_suppliers"] if previous_summary is not None else None
    )
    previous_overdue = (
        previous_summary["overdue_orders"] if previous_summary is not None else None
    )
    previous_delayed_value = (
        previous_summary["delayed_value"] if previous_summary is not None else None
    )
    previous_average_risk = (
        previous_summary["average_risk_score"] if previous_summary is not None else None
    )

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Eligible suppliers", f"{int(current_summary['eligible_suppliers']):,}")
    col2.metric(
        "Attention suppliers",
        f"{int(current_summary['attention_suppliers']):,}",
        delta=format_delta(current_summary["attention_suppliers"], previous_attention),
        delta_color="inverse",
    )
    col3.metric(
        "High-risk suppliers",
        f"{int(current_summary['high_risk_suppliers']):,}",
        delta=format_delta(current_summary["high_risk_suppliers"], previous_high),
        delta_color="inverse",
    )
    col4.metric(
        "Week-end overdue orders",
        f"{int(current_summary['overdue_orders']):,}",
        delta=format_delta(current_summary["overdue_orders"], previous_overdue),
        delta_color="inverse",
    )
    col5.metric(
        "Week-end delayed value",
        format_currency(current_summary["delayed_value"]),
        delta=format_delta(
            current_summary["delayed_value"],
            previous_delayed_value,
            value_type="currency",
        ),
        delta_color="inverse",
    )
    col6.metric(
        "Avg risk score",
        f"{current_summary['average_risk_score']:.3f}",
        delta=format_delta(
            current_summary["average_risk_score"],
            previous_average_risk,
            value_type="score",
            include_percent=False,
        ),
        delta_color="inverse",
    )

    if reporting_days is not None and reporting_days > 0:
        st.caption(f"Reporting days in selected week: {reporting_days:,}")
    st.info(build_executive_insight(current_summary, previous_summary))

    if top_supplier is None:
        if current_summary["medium_risk_suppliers"]:
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
            <p><strong>Risk factors:</strong> {top_supplier["risk_reason"]}</p>
            <p><strong>Operational impact:</strong> {int(top_supplier["total_orders"]):,} orders, {int(top_supplier["overdue_open_orders"]):,} open overdue, {top_supplier["avg_delivery_days"]:.1f} avg delivery days, {top_supplier["avg_delay_days"]:.1f} avg delay days, {format_currency(top_supplier["delayed_order_value"])} delayed value.</p>
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
    driver_chart.update_layout(height=320)
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


def show_trend_charts(df: pd.DataFrame, selected_week_start: date) -> None:
    """Build weekly trend charts from week-end snapshots."""
    complete_week_df = df[df["is_complete_week"]].copy()
    week_end_snapshots = get_week_end_snapshot(complete_week_df)
    if week_end_snapshots.empty:
        st.info("No complete Monday-to-Sunday reporting weeks match the trend filters.")
        return

    weekly_trends = summarize_weekly_trends(week_end_snapshots)
    st.info(build_trend_interpretation(weekly_trends, selected_week_start))

    overdue_orders_chart = px.line(
        weekly_trends,
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
    overdue_max = float(weekly_trends["overdue_open_orders"].max())
    overdue_orders_chart.update_yaxes(range=[0, max(1, overdue_max * 1.12)])

    average_risk_score_chart = px.line(
        weekly_trends,
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
    risk_score_max = float(weekly_trends["risk_score"].max())
    average_risk_score_chart.update_yaxes(range=[0, max(0.1, risk_score_max * 1.18)])

    delayed_order_value_chart = px.line(
        weekly_trends,
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
    delayed_value_max = float(weekly_trends["delayed_order_value"].max())
    delayed_order_value_chart.update_yaxes(range=[0, max(1, delayed_value_max * 1.12)])
    for chart in [
        overdue_orders_chart,
        average_risk_score_chart,
        delayed_order_value_chart,
    ]:
        chart.update_layout(height=360)

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

    supplier_id_column = get_supplier_id_column(df)
    driver_df = (
        df.sort_values("risk_score", ascending=False)
        .head(12)
    )
    driver_df = add_seller_short_label(driver_df)
    seller_short_order = driver_df["seller_short"].tolist()

    # Plotly draws horizontal category axes from bottom to top. Reversing here
    # keeps the highest-risk supplier visually at the top of the chart.
    y_axis_order = seller_short_order[::-1]
    hover_data = {
        supplier_id_column: True,
        "seller_short": False,
    }
    optional_hover_fields = {
        "primary_risk_driver": True,
        "total_orders": True,
        "late_delivery_rate": ":.2f",
        "avg_delivery_days": ":.1f",
        "avg_delay_days": ":.1f",
        "avg_review_score": ":.2f",
        "delayed_order_value": ":,.2f",
    }
    hover_data.update(
        {
            column: value_format
            for column, value_format in optional_hover_fields.items()
            if column in driver_df.columns
        }
    )
    driver_chart = px.bar(
        driver_df,
        x="risk_score",
        y="seller_short",
        orientation="h",
        color="risk_level" if "risk_level" in driver_df.columns else None,
        title="Highest-risk suppliers requiring attention",
        color_discrete_map=RISK_LEVEL_COLORS,
        hover_data=hover_data,
        labels={
            "risk_score": "Risk score",
            "risk_level": "Risk level",
            "seller_short": "Supplier",
            supplier_id_column: "Full supplier ID",
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
    driver_chart.add_vline(
        x=MEDIUM_RISK_THRESHOLD,
        line_dash="dash",
        line_color=RISK_LEVEL_COLORS["medium"],
        annotation_text="Medium threshold 0.40",
        annotation_position="top",
    )
    driver_chart.add_vline(
        x=HIGH_RISK_THRESHOLD,
        line_dash="dash",
        line_color=RISK_LEVEL_COLORS["high"],
        annotation_text="High threshold 0.70",
        annotation_position="top",
    )
    driver_chart.update_layout(
        height=420,
        yaxis={"categoryorder": "array", "categoryarray": y_axis_order},
    )
    max_score = float(driver_df["risk_score"].max()) if "risk_score" in driver_df else 1.0
    driver_chart.update_xaxes(range=[0, max(0.85, max_score * 1.12)])
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

    supplier_id_column = get_supplier_id_column(df)
    breakdown_df = (
        df.sort_values("risk_score", ascending=False)
        .head(5)
        .loc[
            :,
            [
                supplier_id_column,
                "risk_score",
                "risk_score_delivery_component",
                "risk_score_review_component",
                "risk_score_value_component",
            ],
        ]
        .copy()
    )
    breakdown_df = add_seller_short_label(breakdown_df)
    seller_short_order = breakdown_df["seller_short"].tolist()

    # Use the same supplier order as the highest-risk chart above.
    y_axis_order = seller_short_order[::-1]
    breakdown_df = breakdown_df.rename(
        columns={
            "risk_score_delivery_component": "Delivery",
            "risk_score_review_component": "Reviews",
            "risk_score_value_component": "Value exposure",
        }
    )
    breakdown_long = breakdown_df.melt(
        id_vars=["seller_short", supplier_id_column, "risk_score"],
        value_vars=["Delivery", "Reviews", "Value exposure"],
        var_name="risk_component",
        value_name="score_contribution",
    )

    breakdown_chart = px.bar(
        breakdown_long,
        x="score_contribution",
        y="seller_short",
        color="risk_component",
        orientation="h",
        title="Risk score contribution, top 5 attention suppliers",
        color_discrete_map=DRIVER_COLORS,
        hover_data={
            supplier_id_column: True,
            "seller_short": False,
            "risk_score": ":.3f",
        },
        labels={
            "score_contribution": "Weighted score contribution",
            "seller_short": "Supplier",
            supplier_id_column: "Full supplier ID",
            "risk_component": "Risk component",
            "risk_score": "Risk score",
        },
    )
    polish_chart(breakdown_chart)
    breakdown_chart.update_layout(
        barmode="stack",
        yaxis={"categoryorder": "array", "categoryarray": y_axis_order},
    )
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
        "risk_level",
        "risk_score",
        "primary_risk_driver",
        "risk_reason",
        "recommended_action",
        "confidence_flag",
        "total_orders",
        "late_orders",
        "overdue_open_orders",
        "delayed_order_value",
        "late_or_overdue_rate",
        "low_review_rate",
        "delayed_value_share",
        "avg_delivery_days",
    ]

    if df.empty:
        st.info("No attention suppliers match the current filters.")
        return

    action_by_driver = {
        "Delivery": "Review delivery SLA",
        "Reviews": "Check customer feedback",
        "Value exposure": "Prioritize value exposure",
    }
    attention_table = df.copy()
    if "seller_id" in attention_table.columns:
        attention_table["supplier"] = attention_table["seller_id"].map(shorten_identifier)
    else:
        attention_table["supplier"] = "Unknown supplier"
    if "primary_risk_driver" in attention_table.columns:
        attention_table["recommended_action"] = attention_table[
            "primary_risk_driver"
        ].map(action_by_driver).fillna("Monitor next week")
    else:
        attention_table["recommended_action"] = "Monitor next week"
    if "total_orders" in attention_table.columns:
        attention_table["confidence_flag"] = attention_table["total_orders"].map(
            lambda total_orders: "Low volume" if total_orders < 5 else "Normal"
        )
    else:
        attention_table["confidence_flag"] = "Normal"
    if "late_delivery_rate" in attention_table.columns:
        attention_table["late_or_overdue_rate"] = attention_table["late_delivery_rate"] * 100
    if "low_review_rate" in attention_table.columns:
        attention_table["low_review_rate"] = attention_table["low_review_rate"] * 100
    if "delayed_order_value_share" in attention_table.columns:
        attention_table["delayed_value_share"] = attention_table[
            "delayed_order_value_share"
        ] * 100

    low_volume_count = int((attention_table["confidence_flag"] == "Low volume").sum())
    if low_volume_count:
        st.warning(
            f"{low_volume_count:,} attention suppliers have fewer than 5 orders. "
            "Treat these risk scores as lower confidence."
        )

    sort_columns = get_available_columns(
        attention_table,
        ["risk_score", "delayed_order_value"],
    )
    attention_table = attention_table[get_available_columns(attention_table, table_columns)]
    if sort_columns:
        attention_table = attention_table.sort_values(
            by=sort_columns,
            ascending=[False] * len(sort_columns),
        )
    attention_table = attention_table.reset_index(drop=True)

    st.dataframe(
        attention_table,
        use_container_width=True,
        hide_index=True,
        height=520,
        column_config={
            "supplier": st.column_config.TextColumn("supplier"),
            "risk_level": st.column_config.TextColumn("risk_level"),
            "recommended_action": st.column_config.TextColumn("recommended_action"),
            "confidence_flag": st.column_config.TextColumn("confidence_flag"),
            "late_or_overdue_rate": st.column_config.NumberColumn(
                "late_or_overdue_rate",
                format="%.1f%%",
            ),
            "low_review_rate": st.column_config.NumberColumn(
                "low_review_rate",
                format="%.1f%%",
            ),
            "delayed_value_share": st.column_config.NumberColumn(
                "delayed_value_share",
                format="%.1f%%",
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

    previous_report_snapshot_df = get_previous_report_snapshot(
        filtered_df,
        selected_week_start,
    )

    attention_df = report_snapshot_df[
        report_snapshot_df["risk_level"].isin(["high", "medium"])
    ].copy()

    metadata_parts = [
        f"Report week: {selected_week_start} to {selected_week_end}",
        f"Snapshot used: {report_snapshot_date}",
    ]
    if min_total_orders is not None:
        metadata_parts.append(f"Minimum total orders: {min_total_orders}")
    st.caption(". ".join(metadata_parts) + ".")

    summary_tab, drivers_tab, trends_tab, attention_tab = st.tabs(
        [
            "Executive Summary",
            "Risk Drivers",
            "Weekly Trends",
            "Attention Suppliers",
        ]
    )

    with summary_tab:
        show_at_a_glance(
            report_snapshot_df,
            selected_week_df["snapshot_date"].nunique(),
            previous_report_snapshot_df,
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
        with st.expander("Show risk score contribution breakdown"):
            show_risk_score_breakdown(attention_df)

    with trends_tab:
        st.caption(
            "Weekly trends use the selected sidebar filters and compare week-end snapshots only."
        )
        show_trend_charts(filtered_df, selected_week_start)

    with attention_tab:
        show_suppliers_requiring_attention(attention_df)


if __name__ == "__main__":
    main()
