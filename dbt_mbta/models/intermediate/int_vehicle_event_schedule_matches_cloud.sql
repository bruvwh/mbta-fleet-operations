{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_key',
    partition_by={
        "field": "service_date",
        "data_type": "date"
    },
    cluster_by=[
        "trip_id",
        "stop_sequence"
    ]
) }}

with eligible_events as (

    select
        event_key,
        ingestion_timestamp,
        trip_id,
        current_stop_sequence,
        vehicle_timestamp

    from {{ ref('stg_vehicle_events') }}

    where
        schedule_relationship = 'SCHEDULED'

        and trip_id is not null

        and vehicle_timestamp is not null

        and current_stop_sequence is not null

        and current_stop_sequence > 0


        {% if is_incremental() %}

        and ingestion_timestamp >= timestamp_sub(

            (
                select
                    coalesce(
                        max(
                            event_ingestion_timestamp
                        ),
                        timestamp_sub(
                            current_timestamp(),
                            interval 2 hour
                        )
                    )

                from {{ this }}
            ),

            interval 10 minute
        )

        {% else %}

        -- The initial cloud matcher starts with only
        -- recent observations.
        --
        -- Existing PostgreSQL-exported matches continue
        -- to provide the historical backfill.
        and ingestion_timestamp >= timestamp_sub(
            current_timestamp(),
            interval 2 hour
        )

        {% endif %}

),


schedule_stops as (

    select
        feed_checksum,
        service_date,
        trip_id,
        stop_sequence,

        coalesce(
            cast(
                scheduled_arrival_local
                as datetime
            ),

            cast(
                scheduled_departure_local
                as datetime
            )
        ) as scheduled_local_time

    from {{ ref('stg_gtfs_stop_instances') }}

    where
        trip_id is not null

        and stop_sequence is not null

        and (
            scheduled_arrival_local is not null

            or scheduled_departure_local is not null
        )

),


candidate_matches as (

    select
        events.event_key,

        events.ingestion_timestamp
            as event_ingestion_timestamp,

        schedule.feed_checksum,

        schedule.service_date,

        events.trip_id,

        events.current_stop_sequence
            as stop_sequence,

        schedule.scheduled_local_time,

        abs(
            timestamp_diff(

                events.vehicle_timestamp,

                timestamp(
                    schedule.scheduled_local_time,
                    'America/New_York'
                ),

                second
            )
        ) as difference_seconds

    from eligible_events as events

    join schedule_stops as schedule

      on events.trip_id
            = schedule.trip_id

     and events.current_stop_sequence
            = schedule.stop_sequence

    -- A GTFS service day can extend beyond midnight,
    -- so consider the surrounding local service dates.
    where abs(
        date_diff(
            date(
                events.vehicle_timestamp,
                'America/New_York'
            ),
            schedule.service_date,
            day
        )
    ) <= 1

),


ranked_candidates as (

    select
        *,

        row_number() over (

            partition by
                event_key

            order by
                difference_seconds,
                service_date desc,
                feed_checksum

        ) as candidate_rank

    from candidate_matches

)


select
    event_key,

    event_ingestion_timestamp,

    feed_checksum,

    service_date,

    trip_id,

    stop_sequence,

    scheduled_local_time,

    cast(
        difference_seconds
        as float64
    ) as difference_seconds,

    current_timestamp()
        as created_at

from ranked_candidates

where candidate_rank = 1