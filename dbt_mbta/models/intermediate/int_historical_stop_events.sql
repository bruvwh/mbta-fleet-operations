with staged as (

    select *
    from {{ ref('stg_lamp_stop_events') }}

),

with_next_stop as (

    select
        *,

        lead(move_timestamp) over (
            partition by
                service_date,
                trip_id
            order by
                stop_sequence
        ) as estimated_departure_timestamp,

        lead(stop_id) over (
            partition by
                service_date,
                trip_id
            order by
                stop_sequence
        ) as next_stop_id

    from staged

)

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
    stop_timestamp as observed_arrival_timestamp,
    estimated_departure_timestamp,

    next_stop_id,

    travel_time_seconds,
    dwell_time_seconds,

    headway_branch_seconds,
    headway_trunk_seconds,

    scheduled_arrival_seconds,
    scheduled_departure_seconds,
    scheduled_travel_time_seconds,

    scheduled_headway_branch_seconds,
    scheduled_headway_trunk_seconds

from with_next_stop