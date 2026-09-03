{{ config(
    materialized='view'
) }}

select
    feed_checksum,
    service_date,
    trip_id,
    route_id,
    service_id,
    direction_id

from {{ source('mbta', 'gtfs_trip_instances') }}