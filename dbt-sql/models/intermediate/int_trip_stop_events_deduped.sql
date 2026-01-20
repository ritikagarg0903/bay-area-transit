{{ config(
    materialized='incremental',
    unique_key='event_key'
) }}

with stg as (
    select
        event_key,
        ingested_at,
        feed_timestamp,
        source,
        trip_id,
        route_id,
        start_date,
        start_time,
        stop_id,
        stop_sequence,
        event_type,
        event_time,
        delay_sec,
        schedule_relationship
    from {{ ref('stg_trip_updates') }}
    
    -- Enforce incremental processing only for performance optimization
    {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
),

final as (
    select
        event_key,
        route_id,
        trip_id,
        stop_id,
        stop_sequence,
        event_type,
        event_time,
        delay_sec,
        ingested_at,
        feed_timestamp,
        -- convenience columns for aggregations
        date(event_time) as event_date,
        timestamp_trunc(event_time, hour) as event_hour
    from stg
    where delay_sec is not null
    and delay_sec between -1800 and 14400
)

select * from final