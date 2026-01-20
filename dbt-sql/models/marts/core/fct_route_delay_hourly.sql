{{ config(materialized='table') }}

select
  route_id,
  cast(event_hour as timestamp) as event_hour,
  date(event_hour) as date_day,
  num_events,
  avg_delay_sec,
  p50_delay_sec,
  p90_delay_sec,
  delayed_5m_events,
  pct_events_delayed_5m
from {{ ref('int_route_delay_hourly') }}
