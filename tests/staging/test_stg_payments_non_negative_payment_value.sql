select
    batch_date,
    order_id,
    payment_sequential,
    payment_value
from {{ ref('stg_payments') }}
where payment_value < 0
