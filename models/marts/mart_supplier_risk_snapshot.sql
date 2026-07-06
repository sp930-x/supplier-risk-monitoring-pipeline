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
        items.batch_date as snapshot_date,
        items.seller_id,
        items.order_id,
        orders.is_late_delivery,
        orders.is_overdue_open,
        orders.is_delivery_risk,
        orders.delay_days,
        iff(
            orders.order_delivered_customer_date is not null,
            datediff('day', orders.order_purchase_timestamp, orders.order_delivered_customer_date),
            null
        ) as delivery_days,
        iff(
            orders.order_delivered_customer_date is null,
            datediff('day', orders.order_purchase_timestamp, orders.batch_date),
            null
        ) as open_order_age_days,
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

orders_in_snapshot as (
    select
        snapshots.snapshot_date,
        orders.seller_id,
        orders.order_id,
        orders.is_late_delivery,
        orders.is_overdue_open,
        orders.is_delivery_risk,
        orders.delay_days,
        orders.delivery_days,
        orders.open_order_age_days,
        orders.order_value
    from snapshot_dates as snapshots
    inner join order_facts as orders
        on orders.snapshot_date = snapshots.snapshot_date
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
        avg(orders.delivery_days) as avg_delivery_days,
        avg(orders.open_order_age_days) as avg_open_order_age_days,
        avg(reviews.review_score) as avg_review_score,
        count(reviews.order_id) as reviewed_orders,
        sum(coalesce(reviews.is_low_review, 0)) as low_review_count,
        sum(coalesce(reviews.is_low_review, 0))
            / nullif(count(reviews.order_id), 0) as low_review_rate,
        sum(orders.order_value) as total_order_value,
        sum(iff(orders.is_delivery_risk, orders.order_value, 0)) as delayed_order_value,
        sum(iff(orders.is_delivery_risk, orders.order_value, 0))
            / nullif(sum(orders.order_value), 0) as delayed_order_value_share
    from orders_in_snapshot as orders
    left join reviews_at_order_grain as reviews
        on orders.order_id = reviews.order_id
        and orders.snapshot_date = reviews.batch_date
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
        avg_delivery_days,
        avg_open_order_age_days,
        avg_review_score,
        reviewed_orders,
        low_review_count,
        coalesce(low_review_rate, 0) as low_review_rate,
        total_order_value,
        delayed_order_value,
        coalesce(delayed_order_value_share, 0) as delayed_order_value_share,
        coalesce(late_delivery_rate, 0) * 0.4 as risk_score_delivery_component,
        coalesce(low_review_rate, 0) * 0.3 as risk_score_review_component,
        coalesce(delayed_order_value_share, 0) * 0.3 as risk_score_value_component
    from seller_metrics
),

scored_with_total as (
    select
        *,
        risk_score_delivery_component
            + risk_score_review_component
            + risk_score_value_component as raw_risk_score,
        (
            risk_score_delivery_component
            + risk_score_review_component
            + risk_score_value_component
        )
        * case
            when total_orders < 5 then 0.35
            when total_orders < 10 then 0.50
            when total_orders < 15 then 0.85
            else 1.00
        end as risk_score
    from scored
),

labeled as (
    select
        *,
        case
            when risk_score_delivery_component >= risk_score_review_component
                and risk_score_delivery_component >= risk_score_value_component
                then 'Delivery'
            when risk_score_review_component >= risk_score_value_component
                then 'Reviews'
            else 'Value exposure'
        end as primary_risk_driver,
        'Delivery ' || round(late_delivery_rate * 100, 0)::varchar
            || '% | Reviews ' || round(low_review_rate * 100, 0)::varchar
            || '% | Value ' || round(delayed_order_value_share * 100, 0)::varchar
            || '%' as risk_reason
    from scored_with_total
),

reporting_calendar_base as (
    select
        *,
        dateadd('day', 1 - dayofweekiso(snapshot_date), snapshot_date) as report_week_start,
        dateadd(
            'day',
            7 - dayofweekiso(snapshot_date),
            snapshot_date
        ) as report_week_end
    from labeled
),

reporting_week_coverage as (
    select
        report_week_start,
        count(distinct snapshot_date) as reporting_week_snapshot_days
    from reporting_calendar_base
    group by report_week_start
),

reporting_calendar as (
    select
        base.*,
        coverage.reporting_week_snapshot_days
    from reporting_calendar_base as base
    inner join reporting_week_coverage as coverage
        on base.report_week_start = coverage.report_week_start
)

select
    report_week_start,
    report_week_end,
    reporting_week_snapshot_days,
    reporting_week_snapshot_days = 7 as is_complete_reporting_week,
    snapshot_date,
    seller_id,
    total_orders,
    late_orders,
    overdue_open_orders,
    late_delivery_rate,
    avg_delay_days,
    avg_delivery_days,
    avg_open_order_age_days,
    avg_review_score,
    reviewed_orders,
    low_review_count,
    low_review_rate,
    total_order_value,
    delayed_order_value,
    delayed_order_value_share,
    risk_score_delivery_component,
    risk_score_review_component,
    risk_score_value_component,
    risk_score,
    primary_risk_driver,
    risk_reason,
    case
        when risk_score >= 0.7 then 'high'
        when risk_score >= 0.4 then 'medium'
        else 'low'
    end as risk_level
from reporting_calendar
