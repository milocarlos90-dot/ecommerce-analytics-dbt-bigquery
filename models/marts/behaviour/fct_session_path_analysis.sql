with events as (

    select *
    from {{ ref('fct_events') }}

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
left join {{ ref('fct_sessions') }} s
using (journey_session_id)