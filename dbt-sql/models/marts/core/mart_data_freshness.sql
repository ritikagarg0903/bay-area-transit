{{ config(materialized='view') }}

select
  current_timestamp() as checked_at,
  max(feed_timestamp) as last_feed_timestamp,
  timestamp_diff(current_timestamp(), max(feed_timestamp), hour) as hours_since_last_feed
from {{ ref('stg_trip_updates') }}
