{{
    config(
        materialized = "view",
    )
}}

with marketing_ads as (
    select *
    from {{ ref("marketing_ads") }}
),

agg_sessions as (
    select *
    from {{ ref("agg_sessions") }}
)

select
    agg_sessions.session_id,
    agg_sessions.customer_id,
    agg_sessions.started_at,
    agg_sessions.ended_at,
    marketing_ads.utm_source,
    marketing_ads.utm_medium,
    marketing_ads.utm_campain
from agg_sessions join marketing_ads on agg_sessions.ad_id = marketing_ads.ad_id
