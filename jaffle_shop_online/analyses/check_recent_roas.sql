-- Check recent ROAS values
select 
    day,
    utm_source,
    return_on_advertising_spend,
    total_spend,
    attribution_revenue,
    attribution_points
from {{ ref('cpa_and_roas') }}
where day >= dateadd(day, -7, current_date())
order by day desc, utm_source;






