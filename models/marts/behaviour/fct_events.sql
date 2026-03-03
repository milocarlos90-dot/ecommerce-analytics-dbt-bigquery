with events as (

    select *
    from {{ ref('int_events_cleaned') }}

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
        e.is_session_entry_event
        
    from events e
    left join customers c
        on e.customer_id = c.customer_id

    left join products p
        on e.product_id = p.product_id

)

select *
from final