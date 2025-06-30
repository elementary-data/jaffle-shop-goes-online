{{
  config(materialized='view')
}}

select * from {{ ref('historical_orders') }}
union all
select * from {{ ref('real_time_orders') }} 