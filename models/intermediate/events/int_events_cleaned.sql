with events as (

    -- staging already handles typing + basic renaming
    select *
    from {{ ref('stg_events') }}

),

/* -----------------------------------------------------------
   1. Timestamp standardization
----------------------------------------------------------- */

timestamps_standardized as (

    select
        *,
        created_at as event_ts,
        date(created_at) as event_date
    from events

),

/* -----------------------------------------------------------
   2. Remove bot + test traffic
----------------------------------------------------------- */

valid_traffic as (

    select *
    from timestamps_standardized
      where lower(browser) not like '%bot%'
      and lower(traffic_source) != 'test'

),

/* -----------------------------------------------------------
   3. Normalize source session meaning
   (platform-generated journey session)
----------------------------------------------------------- */

journeys_normalized as (

    select
        *,
        session_id as journey_session_id,

    from valid_traffic

),

/* -----------------------------------------------------------
   4. Parse metadata from URI
----------------------------------------------------------- */

metadata_parsed as (

    select
        *,
        regexp_extract(uri, r'^/([^/?]+)') as page_category,
        cast(regexp_extract(uri, r'/product/(\d+)') as int64) as product_id

    from journeys_normalized

),

/* -----------------------------------------------------------
   5. Normalize event semantics
----------------------------------------------------------- */

events_normalized as (

    select
        *,

        case
            when event_type in ('home','department','product')
                then 'browse'
            when event_type = 'cart'
                then 'cart_interaction'
            when event_type = 'purchase'
                then 'conversion'
            when event_type = 'cancel'
                then 'order_management'
            else 'other'
        end as event_category

    from metadata_parsed

),

/* -----------------------------------------------------------
   6. Event flags
----------------------------------------------------------- */

events_flagged as (

    select
        *,

        case when event_category = 'conversion'
            then 1 else 0
        end as is_conversion_event,

        case when session_sequence_number = 1
            then 1 else 0
        end as is_session_entry_event

    from events_normalized

),

/* -----------------------------------------------------------
   7. Session timing helpers
----------------------------------------------------------- */

events_with_session_timing as (

    select
        *,

        min(event_ts) over (
            partition by journey_session_id
        ) as session_start_ts,

        timestamp_diff(
            event_ts,
            min(event_ts) over (partition by journey_session_id),
            second
        ) as seconds_since_session_start  ,

        timestamp_diff(
          event_ts,
          lag(event_ts) over (
            partition by journey_session_id
            order by session_sequence_number
         ),
        second
       ) as seconds_since_previous_event

    from events_flagged

)

select *
from events_with_session_timing
