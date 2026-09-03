with stop_events as (

    select *
    from {{ ref('int_historical_stop_events') }}

),

scheduled_timestamps as (

    select
        *,

        case
            when scheduled_arrival_seconds is not null then
                timestamp(
                    datetime_add(
                        datetime(service_date, time '00:00:00'),
                        interval scheduled_arrival_seconds second
                    ),
                    'America/New_York'
                )
        end as scheduled_arrival_timestamp,

        case
            when scheduled_departure_seconds is not null then
                timestamp(
                    datetime_add(
                        datetime(service_date, time '00:00:00'),
                        interval scheduled_departure_seconds second
                    ),
                    'America/New_York'
                )
        end as scheduled_departure_timestamp

    from stop_events

),

performance as (

    select
        *,

        case
            when
                observed_arrival_timestamp is not null
                and scheduled_arrival_timestamp is not null
            then timestamp_diff(
                observed_arrival_timestamp,
                scheduled_arrival_timestamp,
                second
            )
        end as arrival_delay_seconds,

        case
            when
                estimated_departure_timestamp is not null
                and scheduled_departure_timestamp is not null
            then timestamp_diff(
                estimated_departure_timestamp,
                scheduled_departure_timestamp,
                second
            )
        end as departure_delay_seconds,

        case
            when
                observed_arrival_timestamp is not null
                and estimated_departure_timestamp is not null
            then timestamp_diff(
                estimated_departure_timestamp,
                observed_arrival_timestamp,
                second
            )
        end as estimated_dwell_seconds

    from scheduled_timestamps

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

    observed_arrival_timestamp,
    estimated_departure_timestamp,

    scheduled_arrival_timestamp,
    scheduled_departure_timestamp,

    arrival_delay_seconds,
    departure_delay_seconds,
    estimated_dwell_seconds,

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

from performance