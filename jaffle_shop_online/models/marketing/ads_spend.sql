{{
    config(
        materialized = "view",
    )
}}

with marketing_ads as (
    select *
    from {{ ref("marketing_ads") }}
)

select date(date) as date_day, utm_source, utm_medium, utm_campain, sum(cost) as spend
from marketing_ads
group by date(date), utm_source, utm_medium, utm_campain
