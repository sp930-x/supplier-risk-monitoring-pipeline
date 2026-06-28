USE WAREHOUSE SUPPLIER_RISK_WH;
USE DATABASE SUPPLIER_RISK;
USE SCHEMA ANALYTICS;

-- 1. Top high-risk supplier snapshots
SELECT *
FROM MART_SUPPLIER_RISK_SNAPSHOT
ORDER BY snapshot_date, risk_score DESC
LIMIT 50;

-- 2. Risk level distribution
SELECT
  risk_level,
  COUNT(*) AS supplier_snapshot_count
FROM MART_SUPPLIER_RISK_SNAPSHOT
GROUP BY risk_level
ORDER BY risk_level;

-- 3. Daily risk summary
SELECT
  snapshot_date,
  COUNT(*) AS supplier_count,
  SUM(CASE WHEN risk_level = 'high' THEN 1 ELSE 0 END) AS high_risk_suppliers,
  SUM(CASE WHEN risk_level = 'medium' THEN 1 ELSE 0 END) AS medium_risk_suppliers,
  SUM(CASE WHEN risk_level = 'low' THEN 1 ELSE 0 END) AS low_risk_suppliers,
  ROUND(AVG(risk_score), 3) AS avg_risk_score,
  ROUND(MAX(risk_score), 3) AS max_risk_score
FROM MART_SUPPLIER_RISK_SNAPSHOT
GROUP BY snapshot_date
ORDER BY snapshot_date;

-- 4. Supplier snapshots with highest delayed order value
SELECT
  snapshot_date,
  seller_id,
  total_orders,
  late_orders,
  late_delivery_rate,
  avg_delay_days,
  avg_review_score,
  total_order_value,
  delayed_order_value,
  risk_score,
  risk_level
FROM MART_SUPPLIER_RISK_SNAPSHOT
ORDER BY delayed_order_value DESC
LIMIT 50;