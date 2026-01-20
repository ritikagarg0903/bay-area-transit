{{ config(materialized='table') }}

with d as (
  select * from {{ ref('fct_route_delay_daily') }}
),

with_lags as (
  select
    route_id,
    date_day,
    total_events,
    avg_delay_sec,
    delayed_5m_events,
    pct_events_delayed_5m,

    lag(pct_events_delayed_5m) over (partition by route_id order by date_day) as pct_delayed_prev_day,
    lag(pct_events_delayed_5m, 7) over (partition by route_id order by date_day) as pct_delayed_prev_week,

    lag(avg_delay_sec) over (partition by route_id order by date_day) as avg_delay_prev_day,
    lag(avg_delay_sec, 7) over (partition by route_id order by date_day) as avg_delay_prev_week
  from d
)

select
  *,
  (pct_events_delayed_5m - pct_delayed_prev_day) as dod_pct_delayed,
  (pct_events_delayed_5m - pct_delayed_prev_week) as wow_pct_delayed,
  (avg_delay_sec - avg_delay_prev_day) as dod_avg_delay_sec,
  (avg_delay_sec - avg_delay_prev_week) as wow_avg_delay_sec
from with_lags
