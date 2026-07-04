select
    batch_date,
    order_id,
    order_item_id,
    count(*) as row_count
from {{ ref('stg_order_items') }}
group by
    batch_date,
    order_id,
    order_item_id
having count(*) > 1
