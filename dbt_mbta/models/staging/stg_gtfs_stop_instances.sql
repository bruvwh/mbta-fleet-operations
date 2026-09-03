{{ config(
    materialized='view'
) }}

select
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence,
    stop_id,
    route_id,
    direction_id,
    scheduled_arrival_local,
    scheduled_departure_local,
    arrival_seconds,
    departure_seconds

from {{ source('mbta', 'gtfs_stop_instances') }}