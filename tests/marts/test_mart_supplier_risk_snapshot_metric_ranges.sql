select
    snapshot_date,
    seller_id,
    late_delivery_rate,
    low_review_rate,
    delayed_order_value_share,
    risk_score,
    risk_score_delivery_component,
    risk_score_review_component,
    risk_score_value_component
from {{ ref('mart_supplier_risk_snapshot') }}
where late_delivery_rate not between 0 and 1
    or low_review_rate not between 0 and 1
    or delayed_order_value_share not between 0 and 1
    or risk_score not between 0 and 1
    or risk_score_delivery_component not between 0 and 0.4
    or risk_score_review_component not between 0 and 0.3
    or risk_score_value_component not between 0 and 0.3
