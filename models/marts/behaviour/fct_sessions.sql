{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    partition_by={
      "field": "session_last_event_date",
      "data_type": "date"
    },
    cluster_by=["customer_sk"]
) }}

with events as (

    select *
    from {{ ref('int_events_cleaned') }}

    {% if is_incremental() %}
    where event_date >= date_sub(current_date, interval {{ var('incremental_lookback_days') }} day)
    {% endif %}

),

session_aggregates as (


select
    journey_session_id,

    -- natural key
    min(customer_id) as customer_id,

    -- timing
    min(event_ts) as session_start_ts,
    max(event_ts) as session_end_ts,
    date(min(event_ts)) as session_date,
    max(event_date) as session_last_event_date,

    timestamp_diff(max(event_ts), min(event_ts), second) as session_duration_seconds,
    timestamp_diff(max(event_ts), min(event_ts), minute) as session_duration_minutes,
    timestamp_diff(max(event_ts), min(event_ts), hour) as session_duration_hours,

    -- volume
    count(*) as event_count,

    -- behaviour
    max(is_conversion_event) as has_conversion,

    max(case when is_session_entry_event = 1 then traffic_source end) as traffic_source,
    max(case when is_session_entry_event = 1 then browser end) as browser,

    -- engagement
    count(distinct product_id) as distinct_products_viewed,
    countif(event_type = 'product') as product_view_count,
    countif(event_type = 'cart') as cart_event_count,
    countif(event_type = 'purchase') as purchase_event_count,

    -- navigation
    array_agg(event_type order by event_ts limit 1)[offset(0)] as first_event_type,
    array_agg(event_type order by event_ts desc limit 1)[offset(0)] as last_event_type,

    -- conversion timing
    min(
        case
            when is_conversion_event = 1 then seconds_since_session_start
        end
    ) as time_to_purchase_seconds,

    -- funnel flags
    max(case when event_type = 'home' then 1 else 0 end) as has_home,
    max(case when event_type = 'department' then 1 else 0 end) as has_department,
    max(case when event_type = 'product' then 1 else 0 end) as has_product,
    max(case when event_type = 'cart' then 1 else 0 end) as has_cart,
    max(case when event_type = 'purchase' then 1 else 0 end) as has_purchase,

    -- session depth
    max(session_sequence_number) as max_session_sequence

from events
group by journey_session_id


),

/* -----------------------------------------------------------
2. Derived session classifications
----------------------------------------------------------- */

session_enriched as (


select
    *,

    -- bounce definition
    case
        when event_count = 1 then 1
        else 0
    end as is_bounce,

    -- product engagement
    case
        when product_view_count = 0
        and has_cart = 0
        and has_purchase = 0
        then 1 else 0
    end as is_no_product_engagement,

    -- session behavioural classification
    case
        when has_conversion = 1 then 'conversion'
        when distinct_products_viewed > 1 then 'research'
        when distinct_products_viewed = 1 then 'single_product'
        else 'browse'
    end as session_type,

    case
    when has_purchase = 1 then 'purchase'
    when has_cart = 1 then 'cart_abandon'
    when has_product = 1 then 'product_exit'
    when event_count = 1 then 'bounce'
    else 'browse_exit'
    end as session_outcome,

    -- funnel depth
    greatest(
        has_home * 1,
        has_department * 2,
        has_product * 3,
        has_cart * 4,
        has_purchase * 5
    ) as funnel_depth

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

/* -----------------------------------------------------------
4. Final session fact table
----------------------------------------------------------- */

final as (


select
    s.journey_session_id,  -- PK
    s.customer_id,
    c.customer_sk,

    -- timing
    s.session_start_ts,
    s.session_end_ts,
    s.session_date,
    s.session_last_event_date,
    s.session_duration_seconds,
    s.session_duration_minutes,
    s.session_duration_hours,

    -- session engagement
    s.event_count,
    s.max_session_sequence,

    -- acquisition
    s.traffic_source,
    s.browser,

    -- product behaviour
    s.distinct_products_viewed,
    s.product_view_count,
    s.cart_event_count,
    s.purchase_event_count,

    -- navigation
    s.first_event_type,
    s.last_event_type,

    -- conversion
    s.has_conversion,
    s.time_to_purchase_seconds,

    -- session classification
    s.is_bounce,
    s.is_no_product_engagement,
    s.session_type,

    -- funnel flags
    s.has_home,
    s.has_department,
    s.has_product,
    s.has_cart,
    s.has_purchase,
    s.funnel_depth,
    current_timestamp() as dbt_updated_at

from session_enriched s
left join customers c
    on s.customer_id = c.customer_id


)

select *
from final
