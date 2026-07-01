with snapshot_dates as (
    select distinct batch_date as snapshot_date
    from {{ ref('stg_orders') }}
),

order_items_at_order_seller_grain as (
    select
        batch_date,
        order_id,
        seller_id,
        sum(item_total_value) as order_value
    from {{ ref('stg_order_items') }}
    group by
        batch_date,
        order_id,
        seller_id
),

order_facts as (
    select
        items.batch_date as order_date,
        items.seller_id,
        items.order_id,
        orders.is_late_delivery,
        orders.is_overdue_open,
        orders.is_delivery_risk,
        orders.delay_days,
        items.order_value
    from order_items_at_order_seller_grain as items
    inner join {{ ref('stg_orders') }} as orders
        on items.order_id = orders.order_id
        and items.batch_date = orders.batch_date
),

reviews_at_order_grain as (
    select
        batch_date,
        order_id,
        avg(review_score) as review_score,
        max(iff(is_low_review, 1, 0)) as is_low_review
    from {{ ref('stg_reviews') }}
    group by
        batch_date,
        order_id
),

orders_in_rolling_window as (
    select
        snapshots.snapshot_date,
        orders.order_date,
        orders.seller_id,
        orders.order_id,
        orders.is_late_delivery,
        orders.is_overdue_open,
        orders.is_delivery_risk,
        orders.delay_days,
        orders.order_value
    from snapshot_dates as snapshots
    inner join order_facts as orders
        on orders.order_date between dateadd('day', -6, snapshots.snapshot_date)
            and snapshots.snapshot_date
),

seller_metrics as (
    select
        orders.snapshot_date,
        orders.seller_id,
        count(distinct orders.order_id) as total_orders,
        sum(iff(orders.is_delivery_risk, 1, 0)) as late_orders,
        sum(iff(orders.is_overdue_open, 1, 0)) as overdue_open_orders,
        sum(iff(orders.is_delivery_risk, 1, 0))
            / nullif(count(distinct orders.order_id), 0) as late_delivery_rate,
        avg(orders.delay_days) as avg_delay_days,
        avg(reviews.review_score) as avg_review_score,
        count(reviews.order_id) as reviewed_orders,
        sum(coalesce(reviews.is_low_review, 0)) as low_review_count,
        sum(coalesce(reviews.is_low_review, 0))
            / nullif(count(reviews.order_id), 0) as low_review_rate,
        sum(orders.order_value) as total_order_value,
        sum(iff(orders.is_delivery_risk, orders.order_value, 0)) as delayed_order_value,
        sum(iff(orders.is_delivery_risk, orders.order_value, 0))
            / nullif(sum(orders.order_value), 0) as delayed_order_value_share
    from orders_in_rolling_window as orders
    left join reviews_at_order_grain as reviews
        on orders.order_id = reviews.order_id
        and orders.order_date = reviews.batch_date
    group by
        orders.snapshot_date,
        orders.seller_id
),

scored as (
    select
        snapshot_date,
        seller_id,
        total_orders,
        late_orders,
        overdue_open_orders,
        coalesce(late_delivery_rate, 0) as late_delivery_rate,
        avg_delay_days,
        avg_review_score,
        reviewed_orders,
        low_review_count,
        coalesce(low_review_rate, 0) as low_review_rate,
        total_order_value,
        delayed_order_value,
        coalesce(delayed_order_value_share, 0) as delayed_order_value_share,
        coalesce(late_delivery_rate, 0) * 0.4
            + coalesce(low_review_rate, 0) * 0.3
            + coalesce(delayed_order_value_share, 0) * 0.3 as risk_score
    from seller_metrics
)

select
    snapshot_date,
    seller_id,
    total_orders,
    late_orders,
    overdue_open_orders,
    late_delivery_rate,
    avg_delay_days,
    avg_review_score,
    reviewed_orders,
    low_review_count,
    low_review_rate,
    total_order_value,
    delayed_order_value,
    delayed_order_value_share,
    risk_score,
    case
        when risk_score >= 0.7 then 'high'
        when risk_score >= 0.4 then 'medium'
        else 'low'
    end as risk_level
from scored
