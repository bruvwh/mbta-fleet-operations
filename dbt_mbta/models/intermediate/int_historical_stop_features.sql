with performance as (

    select *
    from {{ ref('int_historical_stop_performance') }}

),

features as (

    select
        *,

        extract(
            hour
            from scheduled_arrival_timestamp
            at time zone 'America/New_York'
        ) as service_hour,

        extract(
            dayofweek
            from service_date
        ) in (1, 7) as is_weekend,

        case
            when
                observed_arrival_timestamp is not null
                and estimated_departure_timestamp is not null
                and estimated_departure_timestamp
                    >= observed_arrival_timestamp
            then true
            else false
        end as has_sensible_stop_timing,

        case
            when
                scheduled_arrival_timestamp is not null
                or scheduled_departure_timestamp is not null
            then true
            else false
        end as has_schedule,

        case
            when
                arrival_delay_seconds is not null
                and observed_arrival_timestamp is not null
                and estimated_departure_timestamp is not null
                and estimated_departure_timestamp
                    >= observed_arrival_timestamp
            then true
            else false
        end as baseline_eligible

    from performance

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

    service_hour,
    is_weekend,

    move_timestamp,

    observed_arrival_timestamp,
    estimated_departure_timestamp,

    scheduled_arrival_timestamp,
    scheduled_departure_timestamp,

    arrival_delay_seconds,
    departure_delay_seconds,
    estimated_dwell_seconds,

    travel_time_seconds,
    dwell_time_seconds,

    headway_branch_seconds,
    headway_trunk_seconds,

    scheduled_arrival_seconds,
    scheduled_departure_seconds,
    scheduled_travel_time_seconds,

    scheduled_headway_branch_seconds,
    scheduled_headway_trunk_seconds,

    next_stop_id,

    has_schedule,
    has_sensible_stop_timing,
    baseline_eligible

from features