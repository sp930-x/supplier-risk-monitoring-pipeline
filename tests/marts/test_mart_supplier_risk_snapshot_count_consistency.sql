select
    snapshot_date,
    seller_id,
    total_orders,
    late_orders,
    overdue_open_orders,
    reviewed_orders,
    low_review_count
from {{ ref('mart_supplier_risk_snapshot') }}
where late_orders > total_orders
    or overdue_open_orders > total_orders
    or low_review_count > reviewed_orders
    or reviewed_orders > total_orders
