select
    snapshot_date,
    seller_id,
    count(*) as row_count
from {{ ref('mart_supplier_risk_snapshot') }}
group by
    snapshot_date,
    seller_id
having count(*) > 1
