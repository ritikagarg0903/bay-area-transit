{{ config(materialized='table') }}

select
  route_id,
  date_day,

  avg(pct_events_delayed_5m) over (
    partition by route_id
    order by date_day
    rows between 6 preceding and current row
  ) as pct_delayed_7d_avg,

  avg(avg_delay_sec) over (
    partition by route_id
    order by date_day
    rows between 6 preceding and current row
  ) as avg_delay_7d_avg

from {{ ref('fct_route_delay_daily') }}
