{{ config(materialized='view') }}

with events as (
  select
    route_id,
    event_hour,
    delay_sec
  from {{ ref('int_trip_stop_events_deduped') }}
  where route_id is not null
    and delay_sec is not null
),

final as (
  select
    route_id,
    event_hour,
    count(*) as num_events,
    avg(delay_sec) as avg_delay_sec,
    approx_quantiles(delay_sec, 100)[offset(50)] as p50_delay_sec,
    approx_quantiles(delay_sec, 100)[offset(90)] as p90_delay_sec,
    sum(case when delay_sec >= 300 then 1 else 0 end) as delayed_5m_events,
    safe_divide(sum(case when delay_sec >= 300 then 1 else 0 end), count(*)) as pct_events_delayed_5m
  from events
  group by 1,2
)

select * from final
