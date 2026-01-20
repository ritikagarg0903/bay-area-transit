{{ config(materialized='table') }}

with h as (
  select
    route_id,
    date(event_hour) as date_day,
    num_events,
    avg_delay_sec,
    delayed_5m_events
  from {{ ref('int_route_delay_hourly') }}
  where route_id is not null
    and event_hour is not null
),

daily as (
  select
    route_id,
    date_day,

    sum(num_events) as total_events,
    safe_divide(sum(avg_delay_sec * num_events), sum(num_events)) as avg_delay_sec,

    sum(delayed_5m_events) as delayed_5m_events,
    safe_divide(sum(delayed_5m_events), sum(num_events)) as pct_events_delayed_5m
  from h
  group by 1,2
)

select * from daily
