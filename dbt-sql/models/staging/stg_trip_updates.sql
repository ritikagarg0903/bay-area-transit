{{ config(materialized='view') }}

with src as (
    select
        ingested_at,
        feed_timestamp,
        source,
        entity_id,
        -- Clean strings and handle empty values
        nullif(trim(trip_id), '') as trip_id,
        nullif(trim(route_id), '') as route_id,
        nullif(trim(start_date), '') as start_date,
        nullif(trim(start_time), '') as start_time,
        nullif(trim(stop_id), '') as stop_id,
        stop_sequence,
        upper(nullif(trim(event_type), '')) as event_type,
        event_time,
        delay_sec,
        schedule_relationship
    from {{ source('bay_area_transit_rt', 'raw_trip_updates') }}
    where event_time is not null
    and nullif(trim(route_id), '') is not null
),

with_keys as (
    select
        *,
        to_hex(md5(concat(
            coalesce(trip_id,''), '-', 
            coalesce(stop_id,''), '-', 
            coalesce(event_type,'')
        ))) as event_key
    from src
    where event_type in ('ARRIVAL', 'DEPARTURE')
),

deduped as (
    select * except (rn)
    from (
        select
            *,
            -- Identify the most recent update for this specific stop event
            row_number() over (
                partition by event_key 
                order by ingested_at desc, feed_timestamp desc
            ) as rn
        from with_keys
    )
    where rn = 1
)

select * from deduped