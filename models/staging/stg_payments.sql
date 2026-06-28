select
    order_id,
    cast(payment_sequential as number) as payment_sequential,
    payment_type,
    cast(payment_installments as number) as payment_installments,
    cast(payment_value as number(12, 2)) as payment_value,
    source_type,
    to_date(batch_date) as batch_date
from {{ source('raw', 'synthetic_payments') }}

