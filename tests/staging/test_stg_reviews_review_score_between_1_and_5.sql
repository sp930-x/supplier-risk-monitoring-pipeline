select
    batch_date,
    review_id,
    order_id,
    review_score
from {{ ref('stg_reviews') }}
where review_score < 1
    or review_score > 5
