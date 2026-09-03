{{ config(
    materialized='view'
) }}

select
    service_date,
    route_id,
    branch_route_id,
    trunk_route_id,
    trip_id,
    stop_id,
    parent_station,
    stop_sequence,
    direction_id,
    direction,
    direction_destination,
    vehicle_id,
    vehicle_label,
    vehicle_consist,
    stop_count,
    start_seconds,
    move_timestamp,
    stop_timestamp,
    travel_time_seconds,
    dwell_time_seconds,
    headway_branch_seconds,
    headway_trunk_seconds,
    scheduled_arrival_seconds,
    scheduled_departure_seconds,
    scheduled_travel_time_seconds,
    scheduled_headway_branch_seconds,
    scheduled_headway_trunk_seconds

from {{ source('mbta', 'lamp_stop_events') }}