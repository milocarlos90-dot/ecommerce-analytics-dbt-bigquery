with events as (

    select
        journey_session_id,
        event_type,
        session_sequence_number
    from {{ ref('fct_events') }}

),

/* -----------------------------------------------------------
   1. Build ordered event path per session
----------------------------------------------------------- */

session_paths as (

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

select *
from session_paths