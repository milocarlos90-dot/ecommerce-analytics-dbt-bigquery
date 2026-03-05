with events as (

    select
        journey_session_id,
        event_ts,
        event_type
    from {{ ref('fct_events') }}

),

/* -----------------------------------------------------------
   1. Find first occurrence of each funnel step
----------------------------------------------------------- */

step_times as (

    select
        journey_session_id,

        min(case when event_type = 'home' then event_ts end) as home_ts,

        min(case when event_type = 'department' then event_ts end) as department_ts,

        min(case when event_type = 'product' then event_ts end) as product_ts,

        min(case when event_type = 'cart' then event_ts end) as cart_ts,

        min(case when event_type = 'purchase' then event_ts end) as purchase_ts

    from events
    group by journey_session_id

),

/* -----------------------------------------------------------
   2. Calculate time between funnel milestones
----------------------------------------------------------- */

time_between_steps as (

    select

        journey_session_id,

        home_ts,
        department_ts,
        product_ts,
        cart_ts,
        purchase_ts,

        timestamp_diff(department_ts, home_ts, second)
            as seconds_home_to_department,

        timestamp_diff(product_ts, department_ts, second)
            as seconds_department_to_product,

        timestamp_diff(cart_ts, product_ts, second)
            as seconds_product_to_cart,

        timestamp_diff(purchase_ts, cart_ts, second)
            as seconds_cart_to_purchase

    from step_times

)

select *
from time_between_steps