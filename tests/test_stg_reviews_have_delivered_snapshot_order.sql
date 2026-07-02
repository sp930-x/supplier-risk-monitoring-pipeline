select
    reviews.batch_date,
    reviews.order_id,
    reviews.review_id
from {{ ref('stg_reviews') }} as reviews
left join {{ ref('stg_orders') }} as orders
    on reviews.batch_date = orders.batch_date
    and reviews.order_id = orders.order_id
where orders.order_id is null
    or orders.order_delivered_customer_date is null
