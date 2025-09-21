{{
    config(
        materialized = "incremental",
        unique_key = ["session_id", "platform"],
    )
}}

with app_sessions as (
    select *
    from {{ source("sessions", "stg_app_sessions") }}
),

website_sessions as (
    select *
    from {{ source("sessions", "stg_website_sessions") }}
)

select *, 'app' as platform
from app_sessions
union all
select *, 'website' as platform
from website_sessions
