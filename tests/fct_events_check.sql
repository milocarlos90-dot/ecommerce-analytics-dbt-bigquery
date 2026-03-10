{{ config(materialized='view') }}

select * 
from {{ ref('int_events_cleaned') }}
where event_date >= date_sub(current_date, interval 60 day)