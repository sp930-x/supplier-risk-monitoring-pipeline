select
    report_week_start,
    report_week_end,
    reporting_week_snapshot_days,
    is_complete_reporting_week
from {{ ref('mart_supplier_risk_snapshot') }}
where dayofweekiso(report_week_start) != 1
    or dayofweekiso(report_week_end) != 7
    or datediff('day', report_week_start, report_week_end) != 6
    or reporting_week_snapshot_days not between 1 and 7
    or is_complete_reporting_week != (reporting_week_snapshot_days = 7)
