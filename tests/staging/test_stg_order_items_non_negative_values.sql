select
    batch_date,
    order_id,
    order_item_id,
    price,
    freight_value
from {{ ref('stg_order_items') }}
where price < 0
    or freight_value < 0
    or item_total_value < 0
