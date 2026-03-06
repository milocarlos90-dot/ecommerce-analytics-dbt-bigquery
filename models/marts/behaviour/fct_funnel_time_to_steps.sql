with events as (

    select
        journey_session_id,
        event_ts,
        event_type
    from {{ ref('fct_events') }}

),

sessions as (

    select
        journey_session_id,
        has_conversion
    from {{ ref('fct_sessions') }}

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

        case
            when department_ts is not null and home_ts is not null
            then timestamp_diff(department_ts, home_ts, second)
        end as seconds_home_to_department,

        case
            when product_ts is not null and department_ts is not null
            then timestamp_diff(product_ts, department_ts, second)
        end as seconds_department_to_product,

        case
            when cart_ts is not null and product_ts is not null
            then timestamp_diff(cart_ts, product_ts, second)
        end as seconds_product_to_cart,

        case
            when purchase_ts is not null and cart_ts is not null
            then timestamp_diff(purchase_ts, cart_ts, second)
        end as seconds_cart_to_purchase,

        case
            when department_ts is not null and home_ts is not null
            then timestamp_diff(department_ts, home_ts, minute)
        end as minutes_home_to_department,

        case
            when product_ts is not null and department_ts is not null
            then timestamp_diff(product_ts, department_ts, minute)
        end as minutes_department_to_product,

        case
            when cart_ts is not null and product_ts is not null
            then timestamp_diff(cart_ts, product_ts, minute)
        end as minutes_product_to_cart,

        case
            when purchase_ts is not null and cart_ts is not null
            then timestamp_diff(purchase_ts, cart_ts, minute)
        end as minutes_cart_to_purchase,

        case
            when department_ts is not null and home_ts is not null
            then timestamp_diff(department_ts, home_ts, hour)
        end as hours_home_to_department,

        case
            when product_ts is not null and department_ts is not null
            then timestamp_diff(product_ts, department_ts, hour)
        end as hours_department_to_product,

        case
            when cart_ts is not null and product_ts is not null
            then timestamp_diff(cart_ts, product_ts, hour)
        end as hours_product_to_cart,

        case
            when purchase_ts is not null and cart_ts is not null
            then timestamp_diff(purchase_ts, cart_ts, hour)
        end as hours_cart_to_purchase

    from step_times

)

select

    t.journey_session_id,
    t.home_ts,
    t.department_ts,
    t.product_ts,
    t.cart_ts,
    t.purchase_ts,

    t.seconds_home_to_department,
    t.seconds_department_to_product,
    t.seconds_product_to_cart,
    t.seconds_cart_to_purchase,
    t.minutes_home_to_department,
    t.minutes_department_to_product,
    t.minutes_product_to_cart,
    t.minutes_cart_to_purchase,
    t.hours_home_to_department,
    t.hours_department_to_product,
    t.hours_product_to_cart,
    t.hours_cart_to_purchase,

    -- useful for Looker charts

    case
        when t.minutes_cart_to_purchase is null then null
        when t.minutes_cart_to_purchase  <= 1 then '0-1 min'
        when t.minutes_cart_to_purchase  <= 2  then '1-2 min'
        when t.minutes_cart_to_purchase  <= 5 then '2-5 min'
        when t.minutes_cart_to_purchase  <= 10 then '5-10 min'
        else '10+ min'
    end as time_to_purchase_minute_bucket,

    case
        when t.hours_cart_to_purchase is null then null
        when t.hours_cart_to_purchase <= 1 then '1 hour'
        when t.hours_cart_to_purchase <= 2 then '1-2 hour'
        when t.hours_cart_to_purchase <= 5 then '2-5 hour'
        when t.hours_cart_to_purchase <= 10 then '5-10 hour'
        else '10+ hour'
    end as time_to_purchase_hour_bucket,

    case
        when t.minutes_cart_to_purchase <= 1 then 1
        when t.minutes_cart_to_purchase <= 2 then 2
        when t.minutes_cart_to_purchase <= 5 then 3
        when t.minutes_cart_to_purchase <= 10 then 4
        else 5
    end as time_to_purchase_minute_bucket_order,

    case
        when t.hours_cart_to_purchase <= 1 then 1
        when t.hours_cart_to_purchase <= 2 then 2
        when t.hours_cart_to_purchase <= 5 then 3
        when t.hours_cart_to_purchase <= 10 then 4
        else 5
    end as time_to_purchase_hour_bucket_order,

    s.has_conversion

from time_between_steps t
left join sessions s
    using (journey_session_id)