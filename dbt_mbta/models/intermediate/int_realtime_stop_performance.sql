{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=[
        'feed_checksum',
        'service_date',
        'trip_id',
        'stop_sequence',
        'stop_id'
    ],
    partition_by={
        "field": "service_date",
        "data_type": "date"
    },
    cluster_by=[
        "trip_id",
        "stop_id"
    ]
) }}


-- ============================================================
-- Incremental realtime stop performance
--
-- int_realtime_stop_events stamps every recomputed stop with
-- updated_at.
--
-- On incremental runs we only recalculate stop rows that were
-- recently changed upstream.
-- ============================================================


with changed_stop_events as (

    select *

    from {{ ref(
        'int_realtime_stop_events'
    ) }}


    {% if is_incremental() %}

    where updated_at >= timestamp_sub(

        coalesce(

            (
                select
                    max(updated_at)

                from {{ this }}
            ),

            timestamp(
                '1970-01-01 00:00:00+00'
            )
        ),

        interval 30 minute
    )

    {% endif %}

),


schedule_stops as (

    select
        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        stop_id,

        route_id,

        direction_id,

        scheduled_arrival_local,

        scheduled_departure_local

    from {{ ref(
        'stg_gtfs_stop_instances'
    ) }}

),


combined as (

    select
        realtime.feed_checksum,

        realtime.service_date,

        realtime.trip_id,

        realtime.stop_sequence,

        realtime.stop_id,


        coalesce(
            realtime.route_id,
            schedule.route_id
        ) as route_id,


        coalesce(
            realtime.direction_id,
            schedule.direction_id
        ) as direction_id,


        realtime.vehicle_id,

        realtime.vehicle_count,


        -- ----------------------------------------------------
        -- scheduled_*_local is stored as a BigQuery TIMESTAMP,
        -- but semantically represents Boston local wall-clock
        -- schedule time.
        --
        -- Convert it to DATETIME first so the displayed
        -- year/month/day/hour/minute/second are preserved,
        -- then interpret that wall-clock value in Boston.
        -- ----------------------------------------------------

        case

            when schedule.scheduled_arrival_local
                is not null

            then timestamp(

                datetime(
                    schedule.scheduled_arrival_local
                ),

                'America/New_York'
            )

        end as scheduled_arrival,


        case

            when schedule.scheduled_departure_local
                is not null

            then timestamp(

                datetime(
                    schedule.scheduled_departure_local
                ),

                'America/New_York'
            )

        end as scheduled_departure,


        realtime.arrival_lower_bound,

        realtime.arrival_upper_bound,

        realtime.arrival_estimate,

        realtime.arrival_bound_method,


        realtime.departure_lower_bound,

        realtime.departure_upper_bound,

        realtime.departure_estimate,


        realtime.arrival_uncertainty_seconds,

        realtime.departure_uncertainty_seconds,


        realtime.stopped_observation_count,

        realtime.total_observation_count,

        realtime.inference_quality

    from changed_stop_events as realtime

    left join schedule_stops as schedule

      on realtime.feed_checksum
            = schedule.feed_checksum

     and realtime.service_date
            = schedule.service_date

     and realtime.trip_id
            = schedule.trip_id

     and realtime.stop_sequence
            = schedule.stop_sequence

     and realtime.stop_id
            = schedule.stop_id

),


performance as (

    select
        *,


        -- ====================================================
        -- Arrival deviations
        -- ====================================================

        case

            when
                arrival_estimate is not null

                and scheduled_arrival is not null

            then timestamp_diff(
                arrival_estimate,
                scheduled_arrival,
                second
            )

        end as arrival_deviation_seconds,


        case

            when
                arrival_lower_bound is not null

                and scheduled_arrival is not null

            then timestamp_diff(
                arrival_lower_bound,
                scheduled_arrival,
                second
            )

        end as arrival_deviation_lower_seconds,


        case

            when
                arrival_upper_bound is not null

                and scheduled_arrival is not null

            then timestamp_diff(
                arrival_upper_bound,
                scheduled_arrival,
                second
            )

        end as arrival_deviation_upper_seconds,


        -- ====================================================
        -- Departure deviations
        -- ====================================================

        case

            when
                departure_estimate is not null

                and scheduled_departure is not null

            then timestamp_diff(
                departure_estimate,
                scheduled_departure,
                second
            )

        end as departure_deviation_seconds,


        case

            when
                departure_lower_bound is not null

                and scheduled_departure is not null

            then timestamp_diff(
                departure_lower_bound,
                scheduled_departure,
                second
            )

        end as departure_deviation_lower_seconds,


        case

            when
                departure_upper_bound is not null

                and scheduled_departure is not null

            then timestamp_diff(
                departure_upper_bound,
                scheduled_departure,
                second
            )

        end as departure_deviation_upper_seconds,


        current_timestamp()
            as updated_at

    from combined

)


select *

from performance