{{
  config(materialized='view')
}}

-- Union of historical and real-time datasets
select *
from {{ ref('historical_orders') }}

union all

select *
from {{ ref('real_time_orders') }} 