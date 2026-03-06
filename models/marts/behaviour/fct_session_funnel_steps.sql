with sessions as (

    select *
    from {{ ref('fct_sessions') }}

),

funnel_steps as (

    select
        s.journey_session_id,
        s.customer_id,
        s.customer_sk,
        s.session_date,
        s.traffic_source,
        s.browser,

        step.funnel_step,
        step.funnel_order,
        step.reached_step

    from sessions s

    cross join unnest([

        struct('Session'    as funnel_step, 0 as funnel_order, 1               as reached_step),
        struct('Home'       as funnel_step, 1 as funnel_order, s.has_home       as reached_step),
        struct('Department' as funnel_step, 2 as funnel_order, s.has_department as reached_step),
        struct('Product'    as funnel_step, 3 as funnel_order, s.has_product    as reached_step),
        struct('Cart'       as funnel_step, 4 as funnel_order, s.has_cart       as reached_step),
        struct('Purchase'   as funnel_step, 5 as funnel_order, s.has_purchase   as reached_step)

    ]) step

)

select 

    journey_session_id,
    customer_id,
    customer_sk,
    session_date,
    traffic_source,
    browser,

    funnel_step,
    funnel_order,
    reached_step,

    if(reached_step = 1, journey_session_id, null) as reached_session_id

from funnel_steps