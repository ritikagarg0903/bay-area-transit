{{ config(materialized='table') }}

select distinct
  route_id
from {{ ref('int_trip_stop_events_deduped') }}
where route_id is not null
