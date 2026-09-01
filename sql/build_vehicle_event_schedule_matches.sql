-- ============================================================
-- Incrementally match realtime VehiclePositions events
-- to scheduled GTFS trip-stop instances.
--
-- Only vehicle_events that do NOT already have a row in
-- vehicle_event_schedule_matches are processed.
-- ============================================================


WITH unmatched_events AS (

    SELECT
        events.event_key,
        events.trip_id,
        events.current_stop_sequence,
        events.vehicle_timestamp

    FROM vehicle_events AS events

    WHERE
        events.schedule_relationship = 'SCHEDULED'

        -- Incremental filter:
        -- do not recompute events already matched.
        AND NOT EXISTS (

            SELECT 1

            FROM vehicle_event_schedule_matches AS existing

            WHERE existing.event_key = events.event_key
        )

),


event_candidates AS (

    SELECT
        events.event_key,
        instances.feed_checksum,
        instances.service_date,
        events.trip_id,
        events.current_stop_sequence,

        events.vehicle_timestamp
            AT TIME ZONE 'America/New_York'
            AS actual_local_time,

        instances.service_date::timestamp
            + (
                COALESCE(
                    stop_times.arrival_seconds,
                    stop_times.departure_seconds
                )
                * INTERVAL '1 second'
            ) AS scheduled_local_time

    FROM unmatched_events AS events

    JOIN gtfs_trip_instances AS instances
        ON events.trip_id = instances.trip_id

    JOIN gtfs_stop_times AS stop_times
        ON instances.feed_checksum = stop_times.feed_checksum
       AND instances.trip_id = stop_times.trip_id
       AND events.current_stop_sequence = stop_times.stop_sequence

    WHERE
        instances.service_date BETWEEN

            (
                events.vehicle_timestamp
                AT TIME ZONE 'America/New_York'
            )::date - 1

            AND

            (
                events.vehicle_timestamp
                AT TIME ZONE 'America/New_York'
            )::date + 1
),


ranked_candidates AS (

    SELECT
        *,

        ABS(
            EXTRACT(
                EPOCH FROM (
                    actual_local_time
                    - scheduled_local_time
                )
            )
        ) AS difference_seconds,

        ROW_NUMBER() OVER (

            PARTITION BY event_key

            ORDER BY
                ABS(
                    EXTRACT(
                        EPOCH FROM (
                            actual_local_time
                            - scheduled_local_time
                        )
                    )
                )
        ) AS candidate_rank

    FROM event_candidates
)


INSERT INTO vehicle_event_schedule_matches (

    event_key,
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence,
    scheduled_local_time,
    difference_seconds
)

SELECT
    event_key,
    feed_checksum,
    service_date,
    trip_id,
    current_stop_sequence,
    scheduled_local_time,
    difference_seconds

FROM ranked_candidates

WHERE candidate_rank = 1

ON CONFLICT (event_key) DO NOTHING;