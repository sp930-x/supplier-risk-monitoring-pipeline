select
    batch_date,
    order_id,
    payment_sequential,
    count(*) as row_count
from {{ ref('stg_payments') }}
group by
    batch_date,
    order_id,
    payment_sequential
having count(*) > 1
