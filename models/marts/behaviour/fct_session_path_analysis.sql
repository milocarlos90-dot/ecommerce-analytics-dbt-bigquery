with events as (

    select *
    from {{ ref('fct_events') }} --incremental 

),

sessions as (

    select
        journey_session_id,
        has_conversion,
        session_duration_seconds
    from {{ ref('fct_sessions') }} -- table

),

/* -----------------------------------------------------------
   1. Build ordered event path per session
----------------------------------------------------------- */

paths as (

    select
        journey_session_id,

        string_agg(
            event_type,
            ' → '
            order by session_sequence_number
        ) as session_path,

        count(*) as event_count

    from events
    group by journey_session_id

),

/* -----------------------------------------------------------
   2. Create path length bucket
----------------------------------------------------------- */

path_buckets as (

    select

        journey_session_id,
        session_path,
        event_count,

        case
            when event_count = 1 then '1 step'
            when event_count between 2 and 3 then '2-3 steps'
            when event_count between 4 and 5 then '4-5 steps'
            when event_count between 6 and 7 then '6-7 steps'
            when event_count between 8 and 10 then '8-10 steps'
            else '10+ steps'
        end as path_length_bucket

    from paths

)

select

    p.journey_session_id,
    p.session_path,
    p.event_count,
    p.path_length_bucket,
    s.has_conversion,
    s.session_duration_seconds,
    current_timestamp() as dbt_updated_at

from path_buckets p
left join sessions s
using (journey_session_id)