{{ config(materialized='table') }}

with dates as (
  select distinct event_date as date_day
  from {{ ref('int_trip_stop_events_deduped') }}
)

select
  date_day,
  extract(year from date_day) as year,
  extract(month from date_day) as month,
  extract(day from date_day) as day,
  extract(dayofweek from date_day) as day_of_week,
  format_date('%A', date_day) as day_name
from dates
