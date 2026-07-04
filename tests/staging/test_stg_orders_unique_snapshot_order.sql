select
    batch_date,
    order_id,
    count(*) as row_count
from {{ ref('stg_orders') }}
group by
    batch_date,
    order_id
having count(*) > 1
