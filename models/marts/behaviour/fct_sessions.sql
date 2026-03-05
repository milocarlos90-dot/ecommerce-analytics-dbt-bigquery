with events as (

    select *
    from {{ ref('int_events_cleaned') }}

),

/* -----------------------------------------------------------
   1. Aggregate to session grain
----------------------------------------------------------- */

session_aggregates as (

    select
        journey_session_id,

        -- natural key
        min(customer_id) as customer_id,

        -- timing
        min(event_ts) as session_start_ts,
        max(event_ts) as session_end_ts,
        date(min(event_ts)) as session_date,

        timestamp_diff(max(event_ts), min(event_ts), second) as session_duration_seconds,
        timestamp_diff(max(event_ts), min(event_ts), minute) as session_duration_minutes,
        timestamp_diff(max(event_ts), min(event_ts), hour) as session_duration_hours,

        -- volume
        count(*) as event_count,

        -- behaviour
        max(is_conversion_event) as has_conversion,

        max(case when is_session_entry_event = 1 then traffic_source end) as traffic_source,
        max(case when is_session_entry_event = 1 then browser end) as browser,

        count(distinct product_id) as distinct_products_viewed,

        min(
            case when is_conversion_event = 1
                 then seconds_since_session_start
            end
        ) as time_to_purchase_seconds

        -- FUNNEL FLAGS
        max(case when event_type = 'home' then 1 else 0 end) as has_home,
        max(case when event_type = 'department' then 1 else 0 end) as has_department,
        max(case when event_type = 'product' then 1 else 0 end) as has_product,
        max(case when event_type = 'cart' then 1 else 0 end) as has_cart,
        max(case when event_type = 'purchase' then 1 else 0 end) as has_purchase

    from events
    group by journey_session_id

),

/* -----------------------------------------------------------
   2. Derived session classifications
----------------------------------------------------------- */

session_enriched as (

    select
        *,

        case when event_count = 1 then 1 else 0 end as is_bounce,

        case
            when has_conversion = 1 then 'conversion'
            when distinct_products_viewed > 1 then 'research'
            when distinct_products_viewed = 1 then 'single_product'
            else 'browse'
        end as session_type

    from session_aggregates

),

/* -----------------------------------------------------------
   3. Join customer dimension
----------------------------------------------------------- */

customers as (

    select
        customer_id,
        customer_sk
    from {{ ref('dim_customers') }}

),

final as (

    select
        s.journey_session_id,  -- PK
        s.customer_id,
        c.customer_sk,

        s.session_start_ts,
        s.session_end_ts,
        s.session_date,
        s.session_duration_seconds,
        s.session_duration_minutes,
        s.session_duration_hours,
        s.event_count,
        s.has_conversion,
        s.traffic_source,
        s.browser,
        s.distinct_products_viewed,
        s.is_bounce,
        s.session_type,
        s.time_to_purchase_seconds

    from session_enriched s
    left join customers c
        on s.customer_id = c.customer_id

)

select *
from final
