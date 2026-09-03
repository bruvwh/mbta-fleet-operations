{{ config(
    materialized='table'
) }}

with primary_baseline as (

    select
        'HOURLY' as baseline_level,

        route_id,
        direction_id,
        stop_id,
        service_hour,
        is_weekend,

        observation_count,

        q1_delay_seconds,
        median_delay_seconds,
        q3_delay_seconds,
        iqr_delay_seconds

    from {{ ref('int_historical_baseline_primary') }}

),

fallback_baseline as (

    select
        'FALLBACK' as baseline_level,

        route_id,
        direction_id,
        stop_id,

        cast(null as int64) as service_hour,

        is_weekend,

        observation_count,

        q1_delay_seconds,
        median_delay_seconds,
        q3_delay_seconds,
        iqr_delay_seconds

    from {{ ref('int_historical_baseline_fallback') }}

),

combined as (

    select *
    from primary_baseline

    union all

    select *
    from fallback_baseline

)

select
    baseline_level,

    route_id,
    direction_id,
    stop_id,
    service_hour,
    is_weekend,

    observation_count,

    q1_delay_seconds,
    median_delay_seconds,
    q3_delay_seconds,
    iqr_delay_seconds,

    q1_delay_seconds
        - (1.5 * iqr_delay_seconds)
        as lower_anomaly_fence_seconds,

    q3_delay_seconds
        + (1.5 * iqr_delay_seconds)
        as upper_anomaly_fence_seconds

from combined