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
        "route_id",
        "trip_id",
        "stop_id"
    ]
) }}


-- ============================================================
-- Incremental realtime stop features
--
-- Only trips whose stop-performance rows were recently updated
-- are recomputed.
--
-- Once a trip is affected, all schedule matches belonging to
-- that trip are used to recompute schedule-match quality
-- statistics.
-- ============================================================


with affected_trips as (

    select distinct
        feed_checksum,
        service_date,
        trip_id

    from {{ ref(
        'int_realtime_stop_performance'
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


affected_performance as (

    select
        performance.*

    from {{ ref(
        'int_realtime_stop_performance'
    ) }} as performance

    join affected_trips as affected

      on performance.feed_checksum
            = affected.feed_checksum

     and performance.service_date
            = affected.service_date

     and performance.trip_id
            = affected.trip_id

),


-- ============================================================
-- Retrieve all schedule-match observations for affected trips.
-- ============================================================

schedule_match_rows as (

    select
        matches.feed_checksum,

        matches.service_date,

        matches.trip_id,

        matches.stop_sequence,

        events.stop_id,

        matches.difference_seconds

    from affected_trips as affected

    join {{ ref(
        'stg_vehicle_event_schedule_matches'
    ) }} as matches

      on affected.feed_checksum
            = matches.feed_checksum

     and affected.service_date
            = matches.service_date

     and affected.trip_id
            = matches.trip_id


    join {{ ref(
        'stg_vehicle_events'
    ) }} as events

      on matches.event_key
            = events.event_key

     and matches.event_ingestion_timestamp
            = events.ingestion_timestamp


    where
        matches.stop_sequence
            is not null

        and events.stop_id
            is not null

        and matches.difference_seconds
            is not null

),


-- ============================================================
-- BigQuery PERCENTILE_CONT is an analytic function.
-- Calculate the median and maximum over each trip-stop group,
-- then collapse to one row per trip-stop.
-- ============================================================

schedule_match_windowed as (

    select
        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        stop_id,


        percentile_cont(
            difference_seconds,
            0.50
        ) over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                stop_sequence,
                stop_id

        ) as median_difference_seconds,


        max(
            difference_seconds
        ) over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                stop_sequence,
                stop_id

        ) as max_difference_seconds

    from schedule_match_rows

),


schedule_match_features as (

    select distinct
        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        stop_id,

        median_difference_seconds,

        max_difference_seconds

    from schedule_match_windowed

),


final_features as (

    select
        performance.feed_checksum,

        performance.service_date,

        performance.trip_id,

        performance.stop_sequence,

        performance.stop_id,


        performance.route_id,

        performance.direction_id,

        performance.vehicle_id,


        performance.scheduled_arrival,

        performance.scheduled_departure,


        performance.arrival_estimate,

        performance.departure_estimate,


        performance.arrival_deviation_seconds,

        performance.arrival_deviation_lower_seconds,

        performance.arrival_deviation_upper_seconds,

        performance.departure_deviation_seconds,


        performance.arrival_uncertainty_seconds,

        performance.departure_uncertainty_seconds,


        performance.inference_quality,


        match_features.median_difference_seconds
            as schedule_match_median_difference_seconds,


        match_features.max_difference_seconds
            as schedule_match_max_difference_seconds,


        case

            when
                match_features.median_difference_seconds
                    > 3600

            then true

            else false

        end as schedule_match_anomaly,


        case

            when performance.scheduled_arrival
                is not null

            then extract(
                hour from datetime(
                    performance.scheduled_arrival,
                    'America/New_York'
                )
            )

        end as service_hour,


        cast(
            format_date(
                '%u',
                performance.service_date
            )
            as int64
        ) as day_of_week,


        cast(
            format_date(
                '%u',
                performance.service_date
            )
            as int64
        ) in (
            6,
            7
        ) as is_weekend,


        case

            when
                performance.arrival_deviation_seconds
                    is not null

                and performance.arrival_uncertainty_seconds
                    is not null

                and performance.arrival_uncertainty_seconds
                    <= 60

                and performance.inference_quality
                    in (
                        'COMPLETE',
                        'PARTIAL'
                    )

                and not (

                    case

                        when
                            match_features
                            .median_difference_seconds
                                > 3600

                        then true

                        else false

                    end
                )

            then true

            else false

        end as scoring_eligible,


        current_timestamp()
            as updated_at

    from affected_performance
        as performance

    left join schedule_match_features
        as match_features

      on performance.feed_checksum
            = match_features.feed_checksum

     and performance.service_date
            = match_features.service_date

     and performance.trip_id
            = match_features.trip_id

     and performance.stop_sequence
            = match_features.stop_sequence

     and performance.stop_id
            = match_features.stop_id

)


select *

from final_features