select
    items.batch_date,
    items.order_id
from {{ ref('stg_order_items') }} as items
left join {{ ref('stg_orders') }} as orders
    on items.batch_date = orders.batch_date
    and items.order_id = orders.order_id
where orders.order_id is null
