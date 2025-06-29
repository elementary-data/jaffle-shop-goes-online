{{
    config(
        materialized = "table",
    )
}}

with ad_spend as (

    select * from {{ ref('ads_spend') }}

),

attribution as (

    select * from {{ ref('attribution_touches') }}

),

sessions as (

    select * from {{ ref('sessions') }}

),

-- aggregate first as this is easier to debug / often leads to fewer fanouts
ad_spend_aggregated as (

    select
        day,
        utm_source,

        sum(spend) as total_spend

    from ad_spend

    group by 1, 2

),

attribution_aggregated as (

    select
        sess.started_at::date as day,
        sess.utm_source,

        sum(linear_points) as attribution_points,
        sum(linear_revenue) as attribution_revenue

    from attribution attr
    inner join sessions sess on (sess.session_id = attr.session_id and sess.customer_id = attr.customer_id)

    group by 1, 2

),

joined as (

    select
        coalesce(attr_agg.day, spend_agg.day) as day,
        coalesce(attr_agg.utm_source, spend_agg.utm_source) as utm_source,
        coalesce(spend_agg.total_spend, 0) as total_spend,
        coalesce(attr_agg.attribution_points, 0) as attribution_points,
        coalesce(attr_agg.attribution_revenue, 0) as attribution_revenue,
        
        -- CPA: only calculate when we have both spend and attribution points
        case 
            when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.attribution_points, 0) > 0 
            then 1.0 * spend_agg.total_spend / attr_agg.attribution_points
            else null
        end as cost_per_acquisition,
        
        -- ROAS: only calculate when we have both spend and revenue
        case 
            when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.attribution_revenue, 0) > 0 
            then 1.0 * attr_agg.attribution_revenue / spend_agg.total_spend
            else null
        end as return_on_advertising_spend

    from attribution_aggregated attr_agg
    full outer join ad_spend_aggregated spend_agg
        on attr_agg.day = spend_agg.day and attr_agg.utm_source = spend_agg.utm_source

)

select * from joined
order by day, utm_source