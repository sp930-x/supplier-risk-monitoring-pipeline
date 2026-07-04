select
    batch_date,
    review_id,
    count(*) as row_count
from {{ ref('stg_reviews') }}
group by
    batch_date,
    review_id
having count(*) > 1
