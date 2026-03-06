with events as (

    select *
    from {{ ref('fct_events') }}

),

sessions as (

    select
        journey_session_id,
        has_conversion,
        session_duration_seconds
    from {{ ref('fct_sessions') }}

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

)

select
    p.journey_session_id,
    p.session_path,
    p.event_count,
    s.has_conversion,
    s.session_duration_seconds

from paths p
left join sessions s
using (journey_session_id)