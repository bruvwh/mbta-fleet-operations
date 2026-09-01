-- ============================================================
-- Incrementally match realtime VehiclePositions events
-- to scheduled GTFS trip-stop instances.
--
-- Incrementality is driven by processed_files.processed_at.
--
-- Small incremental event batches are materialized and
-- ANALYZED before GTFS matching so PostgreSQL has accurate
-- cardinality estimates and chooses indexed lookups instead
-- of scanning the full historical tables.
-- ============================================================


-- ============================================================
-- 1. Ensure pipeline watermark table exists
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_watermarks (

    transformation_name TEXT PRIMARY KEY,

    last_processed_at TIMESTAMPTZ NOT NULL,

    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- 2. Bootstrap schedule-matching watermark
--
-- Existing installations already processed historical files
-- using the previous schedule matcher.
--
-- Fresh installations start at -infinity.
-- ============================================================

INSERT INTO pipeline_watermarks (
    transformation_name,
    last_processed_at
)

SELECT
    'build_vehicle_event_schedule_matches',

    CASE

        WHEN EXISTS (
            SELECT 1
            FROM vehicle_event_schedule_matches
        )

        THEN COALESCE(
            (
                SELECT MAX(processed_at)
                FROM processed_files
            ),
            '-infinity'::timestamptz
        )

        ELSE '-infinity'::timestamptz

    END

ON CONFLICT (transformation_name)
DO NOTHING;


-- ============================================================
-- 3. Capture deterministic processing bounds
--
-- processed_at represents when a raw file entered the
-- database pipeline.
--
-- Using a fixed upper bound prevents files processed while
-- this transformation is running from being accidentally
-- skipped when the watermark advances.
-- ============================================================

DROP TABLE IF EXISTS
    temp_schedule_match_bounds;


CREATE TEMP TABLE
    temp_schedule_match_bounds
ON COMMIT DROP
AS

SELECT

    watermark.last_processed_at,

    COALESCE(
        (
            SELECT MAX(processed_at)
            FROM processed_files
        ),
        watermark.last_processed_at
    ) AS batch_upper_bound

FROM pipeline_watermarks AS watermark

WHERE
    watermark.transformation_name =
        'build_vehicle_event_schedule_matches';


-- ============================================================
-- 4. Materialize newly processed raw snapshots
-- ============================================================

DROP TABLE IF EXISTS
    temp_schedule_match_snapshots;


CREATE TEMP TABLE
    temp_schedule_match_snapshots
ON COMMIT DROP
AS

SELECT DISTINCT
    files.ingestion_timestamp

FROM processed_files AS files

CROSS JOIN
    temp_schedule_match_bounds AS bounds

WHERE
    files.processed_at
        > bounds.last_processed_at

    AND files.processed_at
        <= bounds.batch_upper_bound;


CREATE UNIQUE INDEX
    temp_schedule_match_snapshots_pkey

ON temp_schedule_match_snapshots (
    ingestion_timestamp
);


ANALYZE
    temp_schedule_match_snapshots;


-- ============================================================
-- 5. Materialize unmatched scheduled vehicle events
--
-- This is the key optimization.
--
-- PostgreSQL now receives accurate statistics for the small
-- incremental batch rather than estimating the batch as a
-- large fraction of the complete vehicle_events table.
-- ============================================================

DROP TABLE IF EXISTS
    temp_schedule_match_events;


CREATE TEMP TABLE
    temp_schedule_match_events
ON COMMIT DROP
AS

SELECT

    events.event_key,
    events.trip_id,
    events.current_stop_sequence,
    events.vehicle_timestamp

FROM temp_schedule_match_snapshots
    AS snapshots

JOIN vehicle_events
    AS events

  ON events.ingestion_timestamp
        = snapshots.ingestion_timestamp

WHERE

    events.schedule_relationship
        = 'SCHEDULED'

    AND events.trip_id
        IS NOT NULL

    AND events.current_stop_sequence
        IS NOT NULL

    AND events.vehicle_timestamp
        IS NOT NULL

    AND NOT EXISTS (

        SELECT 1

        FROM vehicle_event_schedule_matches
            AS existing

        WHERE
            existing.event_key
                = events.event_key
    );


CREATE UNIQUE INDEX
    temp_schedule_match_events_pkey

ON temp_schedule_match_events (
    event_key
);


CREATE INDEX
    temp_schedule_match_events_trip_time_idx

ON temp_schedule_match_events (
    trip_id,
    vehicle_timestamp
);


ANALYZE
    temp_schedule_match_events;


-- ============================================================
-- 6. Generate plausible GTFS schedule candidates
--
-- Vehicle timestamps are UTC.
-- GTFS schedules are interpreted in America/New_York time.
--
-- +/- 1 service day handles overnight GTFS service.
-- ============================================================

WITH event_candidates AS (

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


    FROM temp_schedule_match_events
        AS events


    JOIN gtfs_trip_instances
        AS instances

      ON events.trip_id
            = instances.trip_id

     AND instances.service_date BETWEEN

            (
                events.vehicle_timestamp
                AT TIME ZONE 'America/New_York'
            )::date - 1

            AND

            (
                events.vehicle_timestamp
                AT TIME ZONE 'America/New_York'
            )::date + 1


    JOIN gtfs_stop_times
        AS stop_times

      ON instances.feed_checksum
            = stop_times.feed_checksum

     AND instances.trip_id
            = stop_times.trip_id

     AND events.current_stop_sequence
            = stop_times.stop_sequence
),


-- ============================================================
-- 7. Choose closest scheduled candidate
-- ============================================================

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

            PARTITION BY
                event_key

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


-- ============================================================
-- 8. Insert best match
-- ============================================================

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

WHERE
    candidate_rank = 1

ON CONFLICT (event_key)
DO NOTHING;


-- ============================================================
-- 9. Advance watermark to captured upper bound
--
-- Any files processed after batch_upper_bound remain pending
-- for the next schedule-matching run.
-- ============================================================

UPDATE pipeline_watermarks

SET

    last_processed_at = (

        SELECT
            batch_upper_bound

        FROM temp_schedule_match_bounds
    ),

    updated_at = NOW()

WHERE
    transformation_name =
        'build_vehicle_event_schedule_matches';