{{
    config(
        materialized = "table",
    )
}}

with google_ads as (
    select *
    from {{ source("ads", "stg_google_ads") }}
),

facebook_ads as (
    select *
    from {{ source("ads", "stg_facebook_ads") }}
),

instagram_ads as (
    select *
    from {{ source("ads", "stg_instagram_ads") }}
)

select *, 'google' as utm_source
from google_ads
union all
select *, 'facebook' as utm_source
from facebook_ads
union all
select *, 'instagram' as utm_source
from instagram_ads
