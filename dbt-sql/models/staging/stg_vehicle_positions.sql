{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        feed_timestamp,
        source,
        entity_id,
        nullif(trim(vehicle_id), '') as vehicle_id,
        nullif(trim(trip_id), '') as trip_id,
        nullif(trim(route_id), '') as route_id,
        position_lat,
        position_lon,
        bearing,
        speed,
        current_status,
        current_stop_sequence,
        nullif(trim(stop_id), '') as stop_id,
        timestamp
    from {{ source('bay_area_transit_rt', 'raw_vehicle_positions') }}
    where timestamp is not null
    and vehicle_id is not null
),

with_keys as (
    select
        *,
        -- Create a unique key for every event
        -- Vehicle ID + Timestamp is the correct natural key here.
        to_hex(md5(concat(vehicle_id, '-', cast(timestamp as string)))) as vehicle_event_key
    from src
),

deduped as (
    select * except (rn)
    from (
        select
            *,
            -- If we ingested the exact same event twice, keep the one we processed last
            row_number() over (
                partition by vehicle_event_key
                order by ingested_at desc, feed_timestamp desc
            ) as rn
        from with_keys
    )
    where rn = 1
)

select * from deduped