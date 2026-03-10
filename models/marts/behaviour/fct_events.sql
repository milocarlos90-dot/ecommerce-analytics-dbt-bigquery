{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    unique_key='event_id',
    on_schema_change='append_new_columns',

    partition_by={
      "field": "event_date",
      "data_type": "date"
    },

    cluster_by=[
      "customer_sk",
      "product_sk"
    ]
) }}

-- Partitioning, clustering and incremental logic are included for demonstration purposes.
-- In real-world scenarios, these optimizations are usually applied only to much larger tables.

with events as (

    select *
    from {{ ref('int_events_cleaned') }}

    {% if is_incremental() %}
    where event_date >= date_sub(current_date, interval {{ var('incremental_lookback_days') }} day)
    {% endif %}


),

customers as (

    select
        customer_id,
        customer_sk
    from {{ ref('dim_customers') }}

),

products as (

    select
        product_id,
        product_sk
    from {{ ref('dim_products') }}

),


final as (

    select

        -- FACT PK
        e.event_id,

        -- DIM KEYS
        e.customer_id,
        c.customer_sk,
        e.product_id,
        p.product_sk,

        -- TIME
        e.event_ts,
        e.event_date,

        -- EVENT ATTRIBUTES
        e.event_type,
        e.event_category,
        e.page_category,
        e.browser,
        e.traffic_source,

        -- SESSION CONTEXT
        e.journey_session_id,
        e.session_sequence_number,
        e.seconds_since_session_start,
        e.seconds_since_previous_event,

        -- FLAGS
        e.is_conversion_event,
        e.is_session_entry_event,

        current_timestamp() as dbt_updated_at
        
    from events e
    left join customers c
        on e.customer_id = c.customer_id

    left join products p
        on e.product_id = p.product_id

)

select *
from final