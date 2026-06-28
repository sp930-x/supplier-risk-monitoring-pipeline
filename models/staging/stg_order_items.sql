select
    order_id,
    cast(order_item_id as number) as order_item_id,
    product_id,
    seller_id,
    to_timestamp_ntz(shipping_limit_date) as shipping_limit_date,
    cast(price as number(12, 2)) as price,
    cast(freight_value as number(12, 2)) as freight_value,
    source_type,
    to_date(batch_date) as batch_date,
    seller_risk_cluster,
    cast(price as number(12, 2)) + cast(freight_value as number(12, 2)) as item_total_value
from {{ source('raw', 'synthetic_order_items') }}

