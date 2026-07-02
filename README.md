# Supplier Risk Monitoring Pipeline

This project is a daily supplier risk monitoring pipeline.

It simulates an e-commerce operations use case where a business wants to identify suppliers that may need attention because of repeated delivery delays, low customer review scores, or high delayed order value.

The project builds the full data flow from raw event generation to an analytics dashboard:

- generates synthetic Olist-style order, order item, payment, and review data
- validates each generated daily batch before loading
- loads raw data into Snowflake
- transforms raw data into analytics-ready supplier risk snapshots with dbt
- runs dbt tests to check the analytical models
- visualizes the final supplier risk mart in a local Streamlit dashboard

The main output is a daily supplier risk snapshot table that supports a weekly reporting dashboard and helps answer:

- Which suppliers are currently high risk?
- Are high-risk suppliers increasing or decreasing over time?
- Which suppliers combine delivery delays, poor reviews, and meaningful delayed order value?

## Pipeline Overview

```text
Airflow
  -> Synthetic data generation
  -> Local data validation
  -> Snowflake RAW load
  -> dbt run
  -> dbt test
  -> Streamlit dashboard
```

The final dashboard reads from this Snowflake mart table:

```text
SUPPLIER_RISK.ANALYTICS.MART_SUPPLIER_RISK_SNAPSHOT
```

## What The Pipeline Produces

The final mart has one row per `snapshot_date` and `seller_id`.

It includes supplier-level metrics such as:

- total orders
- late or currently overdue delivery rate
- open overdue orders
- average delay days
- average delivery days
- average open order age days
- reviewed orders
- average review score
- delayed order value
- risk scoring components
- risk score
- risk level: `high`, `medium`, or `low`

The mart uses the current daily snapshot for each `snapshot_date`, so open orders, overdue orders, delivered orders, and reviewed orders are interpreted as the supplier state visible on that date.

## Data Design Notes

`batch_date` is used as a daily observation date, also called a snapshot date.

Each generated batch represents the supplier-relevant order states that are observable on that date. The same `order_id` can appear in multiple daily batches as the order moves through its lifecycle from `processing` to `shipped`, `delivered`, and reviewed.

Each batch therefore contains both:

- new orders purchased on the `batch_date`
- recent existing orders that are still open, overdue, delivered, or newly reviewed as of the `batch_date`

The generator follows these rules:

- `order_purchase_timestamp`, `order_approved_at`, carrier handoff dates, actual delivery dates, and review dates must not be after `batch_date`
- `order_estimated_delivery_date` may be after `batch_date`, because an estimated delivery date can be known before delivery happens
- undelivered orders have an empty `order_delivered_customer_date`
- undelivered orders whose estimated delivery date is before `batch_date` are treated as currently overdue
- reviews are generated only for orders that have already been delivered by the snapshot date

The supplier risk model counts both already late deliveries and currently overdue open orders as delivery risk. Review-based risk is calculated from orders that already have reviews.

## Supplier Risk Scoring

The supplier risk score is a transparent baseline score, not a trained statistical model.

It combines three normalized components:

- delivery risk: share of orders that were delivered late or are currently overdue
- review risk: share of reviewed orders with low review scores
- value exposure risk: share of order value tied to late or currently overdue orders

The current formula is:

```text
risk_score =
    late_delivery_rate * 0.4
  + low_review_rate * 0.3
  + delayed_order_value_share * 0.3
```

Risk levels are assigned with simple threshold rules:

- `high`: `risk_score >= 0.7`
- `medium`: `risk_score >= 0.4` and `< 0.7`
- `low`: `risk_score < 0.4`

The weights and thresholds are domain-informed heuristics for a monitoring dashboard. Delivery performance receives the highest weight because the use case focuses on supplier delivery reliability. Review risk is included because repeated low reviews can indicate customer-facing quality issues. Value exposure is included to prioritize delivery problems that affect more business value.

The `high` threshold of `0.7` is intended to flag suppliers with multiple strong risk signals at the same time, for example a high late-or-overdue delivery rate combined with low reviews or high delayed order value exposure. The `medium` threshold of `0.4` catches suppliers with one strong signal or several moderate signals that may need monitoring but not immediate escalation.

There is intentional overlap between delivery risk and value exposure risk: both are affected by late or overdue orders. They answer different business questions, though. `late_delivery_rate` measures how often a supplier has delivery problems, while `delayed_order_value_share` measures how much order value is exposed to those problems. A supplier with a few delayed high-value orders can therefore be prioritized differently from a supplier with many delayed low-value orders.

Review-based risk can be noisy when a supplier has only a few reviewed orders. The dashboard includes total order and reviewed order counts so small-sample cases can be interpreted with caution rather than treated as definitive supplier quality signals.

In a production setting, these weights and thresholds should be calibrated with historical outcomes, business impact, and stakeholder tolerance for false positives. For this project, the scoring logic is kept simple and explainable so the pipeline and dashboard can be reviewed end to end.

## Project Structure

```text
.
+-- dags/
|   +-- supplier_risk_daily_pipeline.py
+-- dashboard/
|   +-- app.py
+-- models/
|   +-- staging/
|   +-- marts/
|   |   +-- mart_supplier_risk_snapshot.sql
|   +-- schema.yml
|   +-- sources.yml
+-- scripts/
|   +-- generate_synthetic_olist_events.py
|   +-- validate_generated_data.py
|   +-- load_raw_to_snowflake.py
|   +-- test_snowflake_connection.py
+-- tests/
|   +-- test_*.sql
+-- dbt_project.yml
+-- docker-compose.yaml
+-- Dockerfile
+-- requirements-airflow.txt
+-- requirements-dashboard.txt
```

## Environment Variables

Create a local `.env` file in the project root.

Required Snowflake values:

```text
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=SUPPLIER_RISK
SNOWFLAKE_SCHEMA=RAW
```

The Streamlit dashboard uses:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
```

The raw loader and Snowflake connection test also support `SNOWFLAKE_SCHEMA`.

## Install Dependencies

For the main data pipeline:

```powershell
pip install -r requirements-airflow.txt
```

For the local dashboard:

```powershell
pip install -r requirements-dashboard.txt
```

## Run The Pipeline Manually

Generate one day of synthetic data:

```powershell
python scripts/generate_synthetic_olist_events.py --olist-dir data/raw/olist --output-dir data/generated --start-date 2026-06-29 --days 1 --min-orders-per-day 70 --max-orders-per-day 130 --seed 42 --overwrite
```

Validate the generated batch:

```powershell
python scripts/validate_generated_data.py --generated-dir data/generated --batch-date 2026-06-29
```

Load the batch into Snowflake RAW tables:

```powershell
python scripts/load_raw_to_snowflake.py --generated-dir data/generated --batch-date 2026-06-29
```

Run dbt models:

```powershell
dbt run
```

Run dbt tests:

```powershell
dbt test
```

## Run With Airflow

The active DAG is:

```text
dags/supplier_risk_daily_pipeline.py
```

The DAG ID is:

```text
supplier_risk_daily_pipeline
```

Start the Dockerized Airflow environment:

```powershell
docker compose build
docker compose up airflow-init
docker compose up -d
```

The DAG runs daily at 09:00 Europe/Berlin time and processes the completed previous day as the `batch_date`.

For example, the run at `2026-07-01 09:00` creates and loads `batch_date=2026-06-30`, then refreshes the dbt mart so the dashboard reflects data up to the previous day.

Manual DAG triggers from the Airflow UI also use the completed previous day, so triggering the DAG on `2026-06-30` creates and loads `batch_date=2026-06-29` instead of today's partial data.

The Airflow task chain is:

```text
generate_day -> validate_day -> load_day -> dbt run -> dbt test
```

## Run The Streamlit Dashboard

The dashboard is located at:

```text
dashboard/app.py
```

Run it locally:

```powershell
pip install -r requirements-dashboard.txt
streamlit run dashboard/app.py
```

The dashboard includes:

- sidebar filters for report week, `risk_level`, and minimum total orders
- an `Executive Summary` tab with KPI cards, the top priority supplier, risk level distribution, and high-risk driver distribution
- a `Risk Drivers` tab showing why suppliers are flagged, including the main driver and weighted risk score breakdown
- a `Weekly Trends` tab for open overdue orders, average risk score, and delayed order value at week-end snapshots
- a `Supplier Detail` tab with high-risk suppliers on the selected report week's latest snapshot

The dashboard applies the sidebar filters consistently across KPIs, charts, and tables. The visual design uses a muted risk palette and gradient-style risk intensity charting so high-risk suppliers stand out without relying on overly saturated colors.

The report week uses `Week 1`, `Week 2`, and similar labels in charts, with the available date range shown in the filter and chart hover details. For partial weeks, the date range ends at the latest available snapshot rather than the calendar week end. This keeps the underlying mart daily while presenting the dashboard in a weekly reporting format.

The dashboard reads from the final dbt mart table in Snowflake. The mart is refreshed by the Airflow pipeline.

## Validation Notes

The pipeline is designed to be rerun safely for the same `batch_date`.

The raw loader validates generated files before loading by default, deletes existing rows for the target batch date, reloads the current files, and writes ingestion metadata to Snowflake.

The generated-data validator also checks the snapshot-date contract: future actual delivery dates, future review dates, and reviews for undelivered orders fail validation. Future estimated delivery dates are allowed.

The dbt tests include snapshot-grain checks so repeated lifecycle records do not create duplicate rows inside a single `snapshot_date`. The final mart is expected to be unique by `snapshot_date` and `seller_id`.

Useful checks:

```powershell
python -m py_compile dags/supplier_risk_daily_pipeline.py
python -m py_compile scripts/generate_synthetic_olist_events.py
python -m py_compile scripts/validate_generated_data.py
python -m py_compile scripts/load_raw_to_snowflake.py
python -m py_compile dashboard/app.py
```

## Main Technologies

- Python
- pandas
- Snowflake
- dbt
- Apache Airflow
- Docker Compose
- Streamlit
- Plotly
