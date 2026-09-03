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
-- Incremental realtime stop inference
--
-- On a full refresh:
--   process all matched vehicle observations.
--
-- On normal incremental runs:
--   1. identify trips with recently created schedule matches
--   2. retrieve ALL matched observations for those trips
--   3. recompute their stop inference
--   4. MERGE those stop rows into the existing table
--
-- The 30-minute overlap protects against timing boundaries
-- between consecutive Airflow/dbt runs.
-- ============================================================


with affected_trips as (

    select distinct
        feed_checksum,
        service_date,
        trip_id

    from {{ ref(
        'stg_vehicle_event_schedule_matches'
    ) }}

    where
        feed_checksum is not null
        and service_date is not null
        and trip_id is not null


    {% if is_incremental() %}

        and created_at >= timestamp_sub(

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


-- ============================================================
-- Retrieve the complete observation history for each affected
-- trip.
--
-- Do NOT filter these observations to the recent time window.
-- LAG/LEAD and first/last STOPPED_AT calculations require the
-- complete affected trip.
-- ============================================================

matched_events as (

    select
        events.event_key,

        events.vehicle_id,

        events.route_id,

        events.direction_id,

        events.stop_id,

        matches.stop_sequence,

        events.current_status,

        events.vehicle_timestamp,

        matches.feed_checksum,

        matches.service_date,

        matches.trip_id,

        matches.scheduled_local_time

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
        events.vehicle_timestamp
            is not null

        and events.stop_id
            is not null

        and matches.stop_sequence
            is not null

),


-- ============================================================
-- Order observations within each vehicle/trip.
-- ============================================================

ordered_events as (

    select
        *,

        lag(
            vehicle_timestamp
        ) over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            order by
                vehicle_timestamp,
                event_key

        ) as previous_timestamp,


        lag(
            current_status
        ) over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            order by
                vehicle_timestamp,
                event_key

        ) as previous_status,


        lag(
            stop_sequence
        ) over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            order by
                vehicle_timestamp,
                event_key

        ) as previous_stop_sequence,


        lag(
            stop_id
        ) over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            order by
                vehicle_timestamp,
                event_key

        ) as previous_stop_id,


        lead(
            vehicle_timestamp
        ) over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            order by
                vehicle_timestamp,
                event_key

        ) as next_timestamp,


        lead(
            current_status
        ) over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            order by
                vehicle_timestamp,
                event_key

        ) as next_status,


        lead(
            stop_sequence
        ) over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            order by
                vehicle_timestamp,
                event_key

        ) as next_stop_sequence

    from matched_events

),


-- ============================================================
-- Rank STOPPED_AT observations for each trip-stop.
--
-- current_status:
--   0 = INCOMING_AT
--   1 = STOPPED_AT
--   2 = IN_TRANSIT_TO
-- ============================================================

ranked_stopped_events as (

    select
        *,

        row_number() over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                stop_sequence,
                stop_id

            order by
                vehicle_timestamp,
                event_key

        ) as stopped_rank_first,


        row_number() over (

            partition by
                feed_checksum,
                service_date,
                trip_id,
                stop_sequence,
                stop_id

            order by
                vehicle_timestamp desc,
                event_key desc

        ) as stopped_rank_last

    from ordered_events

    where current_status = 1

),


-- ============================================================
-- Summarize observations at the trip-stop grain.
-- ============================================================

stop_summary as (

    select
        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        stop_id,


        max(
            route_id
        ) as route_id,


        max(
            direction_id
        ) as direction_id,


        count(
            distinct vehicle_id
        ) as vehicle_count,


        case

            when count(
                distinct vehicle_id
            ) = 1

            then max(
                vehicle_id
            )

            else null

        end as vehicle_id,


        min(
            scheduled_local_time
        ) as scheduled_local_time,


        count(*)
            as total_observation_count,


        countif(
            current_status = 1
        ) as stopped_observation_count,


        countif(

            current_status = 1

            and (

                previous_status
                    is null

                or previous_status
                    != 1

                or previous_stop_sequence
                    != stop_sequence

                or previous_stop_id
                    != stop_id
            )

        ) as stopped_period_count

    from ordered_events

    group by
        feed_checksum,
        service_date,
        trip_id,
        stop_sequence,
        stop_id

),


-- ============================================================
-- Infer arrival bounds.
--
-- Best case:
-- moving toward this same stop immediately before STOPPED_AT.
--
-- Fallback:
-- previous observation belonged to an earlier stop.
-- ============================================================

first_stopped as (

    select
        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        stop_id,


        vehicle_timestamp
            as first_stopped_timestamp,


        case

            when
                previous_status in (
                    0,
                    2
                )

                and previous_stop_sequence
                    = stop_sequence

                and previous_stop_id
                    = stop_id

            then previous_timestamp


            when
                previous_stop_sequence
                    is not null

                and previous_stop_sequence
                    < stop_sequence

            then previous_timestamp


            else null

        end as arrival_lower_bound,


        case

            when
                previous_status in (
                    0,
                    2
                )

                and previous_stop_sequence
                    = stop_sequence

                and previous_stop_id
                    = stop_id

            then 'SAME_STOP_TRANSITION'


            when
                previous_stop_sequence
                    is not null

                and previous_stop_sequence
                    < stop_sequence

            then 'PREVIOUS_STOP'


            else 'UNBOUNDED'

        end as arrival_bound_method

    from ranked_stopped_events

    where stopped_rank_first = 1

),


-- ============================================================
-- Infer departure bounds.
-- ============================================================

last_stopped as (

    select
        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        stop_id,


        vehicle_timestamp
            as last_stopped_timestamp,


        case

            when
                next_status in (
                    0,
                    2
                )

                and next_stop_sequence
                    >= stop_sequence

            then next_timestamp

            else null

        end as departure_upper_bound

    from ranked_stopped_events

    where stopped_rank_last = 1

),


-- ============================================================
-- Final inferred trip-stop rows.
-- ============================================================

final_events as (

    select
        summary.feed_checksum,

        summary.service_date,

        summary.trip_id,

        summary.stop_sequence,

        summary.stop_id,

        summary.route_id,

        summary.direction_id,

        summary.vehicle_id,

        summary.vehicle_count,

        summary.scheduled_local_time,


        first_stop.arrival_lower_bound,


        first_stop.first_stopped_timestamp
            as arrival_upper_bound,


        first_stop.first_stopped_timestamp
            as arrival_estimate,


        coalesce(
            first_stop.arrival_bound_method,
            'NO_STOP_OBSERVATION'
        ) as arrival_bound_method,


        last_stop.last_stopped_timestamp
            as departure_lower_bound,


        last_stop.departure_upper_bound,


        last_stop.departure_upper_bound
            as departure_estimate,


        case

            when
                first_stop.arrival_lower_bound
                    is not null

                and first_stop.first_stopped_timestamp
                    is not null

            then timestamp_diff(

                first_stop.first_stopped_timestamp,

                first_stop.arrival_lower_bound,

                second
            )

        end as arrival_uncertainty_seconds,


        case

            when
                last_stop.last_stopped_timestamp
                    is not null

                and last_stop.departure_upper_bound
                    is not null

            then timestamp_diff(

                last_stop.departure_upper_bound,

                last_stop.last_stopped_timestamp,

                second
            )

        end as departure_uncertainty_seconds,


        summary.stopped_observation_count,

        summary.total_observation_count,


        case

            when summary.vehicle_count > 1
                then 'MULTIPLE_VEHICLES'

            when summary.stopped_observation_count = 0
                then 'NO_STOP_OBSERVATION'

            when summary.stopped_period_count > 1
                then 'MULTIPLE_STOP_PERIODS'

            when
                first_stop.arrival_lower_bound
                    is not null

                and last_stop.departure_upper_bound
                    is not null

                then 'COMPLETE'

            else 'PARTIAL'

        end as inference_quality,


        current_timestamp()
            as updated_at

    from stop_summary as summary

    left join first_stopped as first_stop

      using (
          feed_checksum,
          service_date,
          trip_id,
          stop_sequence,
          stop_id
      )

    left join last_stopped as last_stop

      using (
          feed_checksum,
          service_date,
          trip_id,
          stop_sequence,
          stop_id
      )

)


select *

from final_events