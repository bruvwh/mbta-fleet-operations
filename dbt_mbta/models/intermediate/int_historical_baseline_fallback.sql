{{ config(
    materialized='table'
) }}

with eligible as (

    select
        route_id,
        direction_id,
        stop_id,
        is_weekend,
        arrival_delay_seconds

    from {{ ref('int_historical_stop_features') }}

    where baseline_eligible = true
      and route_id is not null
      and direction_id is not null
      and stop_id is not null
      and arrival_delay_seconds is not null

),

aggregated as (

    select
        route_id,
        direction_id,
        stop_id,
        is_weekend,

        count(*) as observation_count,

        approx_quantiles(
            arrival_delay_seconds,
            100
        )[offset(25)] as q1_delay_seconds,

        approx_quantiles(
            arrival_delay_seconds,
            100
        )[offset(50)] as median_delay_seconds,

        approx_quantiles(
            arrival_delay_seconds,
            100
        )[offset(75)] as q3_delay_seconds

    from eligible

    group by
        route_id,
        direction_id,
        stop_id,
        is_weekend

)

select
    route_id,
    direction_id,
    stop_id,
    is_weekend,

    observation_count,

    q1_delay_seconds,
    median_delay_seconds,
    q3_delay_seconds,

    q3_delay_seconds - q1_delay_seconds
        as iqr_delay_seconds

from aggregated

where observation_count >= 30