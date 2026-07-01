with source_orders as (
    select *
    from {{ source('raw', 'synthetic_orders') }}
),

typed_orders as (
    select
        order_id,
        customer_id,
        order_status,
        to_timestamp_ntz(order_purchase_timestamp) as order_purchase_timestamp,
        to_timestamp_ntz(order_approved_at) as order_approved_at,
        to_timestamp_ntz(order_delivered_carrier_date) as order_delivered_carrier_date,
        to_timestamp_ntz(order_delivered_customer_date) as order_delivered_customer_date,
        to_timestamp_ntz(order_estimated_delivery_date) as order_estimated_delivery_date,
        source_type,
        to_date(batch_date) as batch_date
    from source_orders
),

delivery_status as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        source_type,
        batch_date,
        order_delivered_customer_date is not null
            and datediff('day', order_estimated_delivery_date, order_delivered_customer_date) > 0
            as is_late_delivery,
        order_delivered_customer_date is null
            and to_date(order_estimated_delivery_date) < batch_date
            as is_overdue_open
    from typed_orders
)

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    source_type,
    batch_date,
    is_late_delivery,
    is_overdue_open,
    is_late_delivery or is_overdue_open as is_delivery_risk,
    case
        when order_delivered_customer_date is not null then greatest(
            coalesce(datediff('day', order_estimated_delivery_date, order_delivered_customer_date), 0),
            0
        )
        when is_overdue_open then datediff('day', order_estimated_delivery_date, batch_date)
        else 0
    end as delay_days
from delivery_status
