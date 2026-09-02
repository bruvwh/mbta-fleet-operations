-- ============================================================
-- Incremental trip realtime summary
--
-- Recalculate summaries only for trips affected by new
-- schedule matches.
--
-- IMPORTANT:
-- Once a trip is affected, ALL matched observations for that
-- trip are included so the summary represents the complete
-- realtime history currently available for that trip.
--
-- Performance strategy:
--   1. Capture a fixed processing window.
--   2. ANALYZE the one-row bounds table.
--   3. Materialize affected trips.
--   4. Materialize all matches for those trips.
--   5. Join to partitioned vehicle_events_v2 using:
--
--          event_key
--          event_ingestion_timestamp
--
--   6. Advance the watermark only to the fixed upper bound.
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
-- 2. Bootstrap watermark
-- ============================================================

INSERT INTO pipeline_watermarks (
    transformation_name,
    last_processed_at
)

SELECT

    'build_trip_realtime_summary',

    CASE

        WHEN EXISTS (
            SELECT 1
            FROM trip_realtime_summary
        )

        THEN COALESCE(
            (
                SELECT MAX(created_at)
                FROM vehicle_event_schedule_matches
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
-- The upper bound is fixed before processing starts.
--
-- ANALYZE is important because PostgreSQL otherwise may
-- badly overestimate this one-row temporary table and choose
-- an inefficient plan against vehicle_event_schedule_matches.
-- ============================================================

DROP TABLE IF EXISTS
    temp_trip_summary_bounds;


CREATE TEMP TABLE
    temp_trip_summary_bounds
ON COMMIT DROP
AS

SELECT

    watermark.last_processed_at,

    COALESCE(
        (
            SELECT MAX(created_at)
            FROM vehicle_event_schedule_matches
        ),
        watermark.last_processed_at
    ) AS batch_upper_bound

FROM pipeline_watermarks AS watermark

WHERE
    watermark.transformation_name =
        'build_trip_realtime_summary';


ANALYZE
    temp_trip_summary_bounds;


-- ============================================================
-- 4. Materialize trips affected by NEW schedule matches
-- ============================================================

DROP TABLE IF EXISTS
    temp_trip_summary_affected_trips;


CREATE TEMP TABLE
    temp_trip_summary_affected_trips
ON COMMIT DROP
AS

SELECT DISTINCT

    matches.feed_checksum,

    matches.service_date,

    matches.trip_id

FROM vehicle_event_schedule_matches
    AS matches

CROSS JOIN temp_trip_summary_bounds
    AS bounds

WHERE

    matches.created_at
        > bounds.last_processed_at

    AND matches.created_at
        <= bounds.batch_upper_bound;


CREATE UNIQUE INDEX
    temp_trip_summary_affected_trips_pkey

ON temp_trip_summary_affected_trips (
    feed_checksum,
    service_date,
    trip_id
);


ANALYZE
    temp_trip_summary_affected_trips;


-- ============================================================
-- 5. Retrieve ALL schedule matches for affected trips
--
-- This intentionally includes older observations belonging to
-- an affected trip so MIN/MAX/count calculations describe the
-- complete trip rather than only the new incremental batch.
-- ============================================================

DROP TABLE IF EXISTS
    temp_trip_summary_matches;


CREATE TEMP TABLE
    temp_trip_summary_matches
ON COMMIT DROP
AS

SELECT

    matches.event_key,

    matches.event_ingestion_timestamp,

    matches.feed_checksum,

    matches.service_date,

    matches.trip_id

FROM temp_trip_summary_affected_trips
    AS affected

JOIN vehicle_event_schedule_matches
    AS matches

  ON matches.feed_checksum
        = affected.feed_checksum

 AND matches.service_date
        = affected.service_date

 AND matches.trip_id
        = affected.trip_id;


CREATE UNIQUE INDEX
    temp_trip_summary_matches_event_idx

ON temp_trip_summary_matches (
    event_key,
    event_ingestion_timestamp
);


CREATE INDEX
    temp_trip_summary_matches_trip_idx

ON temp_trip_summary_matches (
    feed_checksum,
    service_date,
    trip_id
);


ANALYZE
    temp_trip_summary_matches;


-- ============================================================
-- 6. Recalculate complete summaries for affected trips
--
-- vehicle_events_v2 is partitioned by ingestion_timestamp.
--
-- Schedule matches retain both parts needed to identify the
-- corresponding vehicle observation:
--
--     event_key
--     event_ingestion_timestamp
-- ============================================================

WITH affected_trip_summary AS (

    SELECT

        matches.feed_checksum,

        matches.service_date,

        matches.trip_id,


        instances.route_id,

        instances.direction_id,


        MIN(events.vehicle_timestamp)
            AS first_observation,


        MAX(events.vehicle_timestamp)
            AS last_observation,


        COUNT(*)
            AS observation_count,


        COUNT(DISTINCT events.vehicle_id)
            AS distinct_vehicle_count,


        MIN(events.current_stop_sequence)
            AS first_stop_sequence,


        MAX(events.current_stop_sequence)
            AS last_stop_sequence


    FROM temp_trip_summary_matches
        AS matches


    JOIN vehicle_events_v2
        AS events

      ON events.event_key
            = matches.event_key

     AND events.ingestion_timestamp
            = matches.event_ingestion_timestamp


    JOIN gtfs_trip_instances
        AS instances

      ON instances.feed_checksum
            = matches.feed_checksum

     AND instances.service_date
            = matches.service_date

     AND instances.trip_id
            = matches.trip_id


    GROUP BY

        matches.feed_checksum,

        matches.service_date,

        matches.trip_id,

        instances.route_id,

        instances.direction_id
)


-- ============================================================
-- 7. Upsert affected trip summaries
-- ============================================================

INSERT INTO trip_realtime_summary (

    feed_checksum,

    service_date,

    trip_id,

    route_id,

    direction_id,

    first_observation,

    last_observation,

    observation_count,

    distinct_vehicle_count,

    first_stop_sequence,

    last_stop_sequence
)

SELECT

    feed_checksum,

    service_date,

    trip_id,

    route_id,

    direction_id,

    first_observation,

    last_observation,

    observation_count,

    distinct_vehicle_count,

    first_stop_sequence,

    last_stop_sequence

FROM affected_trip_summary


ON CONFLICT (
    feed_checksum,
    service_date,
    trip_id
)

DO UPDATE SET

    route_id =
        EXCLUDED.route_id,

    direction_id =
        EXCLUDED.direction_id,

    first_observation =
        EXCLUDED.first_observation,

    last_observation =
        EXCLUDED.last_observation,

    observation_count =
        EXCLUDED.observation_count,

    distinct_vehicle_count =
        EXCLUDED.distinct_vehicle_count,

    first_stop_sequence =
        EXCLUDED.first_stop_sequence,

    last_stop_sequence =
        EXCLUDED.last_stop_sequence,

    updated_at = NOW();


-- ============================================================
-- 8. Advance watermark
--
-- Advance only to the upper bound captured before processing
-- began.
--
-- New schedule matches created while this transformation is
-- running remain available for the next pipeline run.
-- ============================================================

UPDATE pipeline_watermarks

SET

    last_processed_at = (

        SELECT
            batch_upper_bound

        FROM temp_trip_summary_bounds
    ),

    updated_at = NOW()

WHERE
    transformation_name =
        'build_trip_realtime_summary';