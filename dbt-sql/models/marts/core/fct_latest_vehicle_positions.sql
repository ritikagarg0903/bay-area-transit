{{ config(
    materialized='table',
    tags=['vehicle_positions']
) }}

with latest_positions as (
    select
        vehicle_id,
        trip_id,
        route_id,
        position_lat,
        position_lon,
        bearing,
        speed,
        timestamp as last_updated_at,
        timestamp_diff(current_timestamp(), timestamp, minute) as minutes_since_update
    from {{ ref('stg_vehicle_positions') }}
    where position_lat is not null 
    and position_lon is not null
    -- Get the single latest ping per vehicle
    qualify row_number() over (partition by vehicle_id order by timestamp desc) = 1
),

latest_delays as (
    select
        trip_id,
        -- If a trip has multiple future stops, we take the delay 
        -- from the most recently received update for that trip.
        delay_sec
    from {{ ref('stg_trip_updates') }}
    where trip_id is not null
    and delay_sec is not null
    -- Get the latest prediction available for this trip
    qualify row_number() over (partition by trip_id order by ingested_at desc, feed_timestamp desc) = 1
)

select
    p.vehicle_id,
    p.trip_id,
    p.route_id,
    p.position_lat,
    p.position_lon,
    p.bearing,
    p.speed,
    p.last_updated_at,
    p.minutes_since_update,
    -- Join the delay data. If NULL (no prediction), assume 0 (On Time).
    coalesce(d.delay_sec, 0) as current_delay_sec
from latest_positions p
left join latest_delays d 
    on p.trip_id = d.trip_id