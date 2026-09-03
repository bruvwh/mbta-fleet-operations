{{ config(
    materialized='table'
) }}

with stop_features as (

    select *
    from {{ ref('int_historical_stop_features') }}

),

eligible as (

    select
        service_date,
        route_id,
        direction_id,
        trip_id,
        stop_id,
        departure_delay_seconds

    from stop_features

    where baseline_eligible = true
      and route_id is not null
      and direction_id is not null
      and trip_id is not null
      and departure_delay_seconds is not null

),

aggregated as (

    select
        service_date,
        route_id,
        direction_id,

        count(*) as stop_observation_count,

        count(distinct trip_id) as trip_count,

        count(distinct stop_id) as distinct_stop_count,

        avg(departure_delay_seconds)
            as avg_departure_delay_seconds,

        approx_quantiles(
            departure_delay_seconds,
            100
        )[offset(50)] as median_departure_delay_seconds,

        approx_quantiles(
            departure_delay_seconds,
            100
        )[offset(90)] as p90_departure_delay_seconds,

        avg(abs(departure_delay_seconds))
            as avg_absolute_departure_delay_seconds

    from eligible

    group by
        service_date,
        route_id,
        direction_id

)

select *
from aggregated