select
    payments.batch_date,
    payments.order_id
from {{ ref('stg_payments') }} as payments
left join {{ ref('stg_orders') }} as orders
    on payments.batch_date = orders.batch_date
    and payments.order_id = orders.order_id
where orders.order_id is null
