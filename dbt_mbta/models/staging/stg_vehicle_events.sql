{{ config(
    materialized='view'
) }}

select
    event_key,
    entity_id,

    vehicle_id,
    trip_id,
    route_id,

    schedule_relationship,
    direction_id,

    latitude,
    longitude,

    stop_id,
    current_stop_sequence,
    current_status,

    vehicle_timestamp,
    feed_timestamp,
    ingestion_timestamp,
    created_at

from {{ source('mbta', 'vehicle_events') }}