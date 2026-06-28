select
    review_id,
    order_id,
    cast(review_score as number) as review_score,
    to_timestamp_ntz(review_creation_date) as review_creation_date,
    to_timestamp_ntz(review_answer_timestamp) as review_answer_timestamp,
    source_type,
    to_date(batch_date) as batch_date,
    cast(review_score as number) <= 2 as is_low_review
from {{ source('raw', 'synthetic_reviews') }}

