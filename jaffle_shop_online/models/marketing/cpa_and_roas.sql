{{
    config(
        materialized = "table"
    )
}}

with ad_spend_raw as (

    select * from {{ ref('ads_spend') }}

),

attribution as (

    select * from {{ ref('attribution_touches') }}

),

-- Step 1: Aggregate spend per (month, source)
ad_spend_monthly as (

    select
        date_trunc('month', date_day) as date_month,
        utm_source,
        sum(spend) as total_spend
    from ad_spend_raw
    group by 1, 2

),

-- Step 2: Assign rank to months
ad_spend_ranked as (

    select *,
        dense_rank() over (order by date_month desc) as month_rank
    from ad_spend_monthly

),

-- Step 3: We filter rows where month_rank > 1 so we can exclude the current month
ad_spend_filtered as (

    select date_month, utm_source, total_spend
    from ad_spend_ranked
    where month_rank > 1

),

-- Step 4: Aggregate attribution per (month, source)
attribution_aggregated as (

    select
        date_trunc('month', converted_at) as date_month,
        utm_source,
        sum(linear_points) as attribution_points,
        sum(linear_revenue) as attribution_revenue
    from attribution
    group by 1, 2

),

-- Step 5: Join attribution with filtered spend
joined as (

    select
        a.date_month,
        a.utm_source,
        a.attribution_points,
        a.attribution_revenue,
        s.total_spend,

        1.0 * nullif(s.total_spend, 0) / a.attribution_points as cost_per_acquisition,
        1.0 * a.attribution_revenue / nullif(s.total_spend, 0) as return_on_advertising_spend

    from attribution_aggregated a
    left join ad_spend_filtered s
      on a.date_month = s.date_month
     and a.utm_source = s.utm_source

)

select * from joined
order by date_month, utm_source
