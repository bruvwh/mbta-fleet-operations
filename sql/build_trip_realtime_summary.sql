-- ============================================================
-- Incremental trip realtime summary
--
-- Recalculate summaries only for trips affected by new
-- schedule matches.
-- ============================================================


CREATE TABLE IF NOT EXISTS pipeline_watermarks (

    transformation_name TEXT PRIMARY KEY,

    last_processed_at TIMESTAMPTZ NOT NULL,

    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- ------------------------------------------------------------
-- Bootstrap watermark
-- ------------------------------------------------------------

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
-- Identify trips affected by new schedule matches
-- ============================================================

WITH watermark AS (

    SELECT last_processed_at

    FROM pipeline_watermarks

    WHERE transformation_name =
        'build_trip_realtime_summary'
),


affected_trips AS (

    SELECT DISTINCT
        matches.feed_checksum,
        matches.service_date,
        matches.trip_id

    FROM vehicle_event_schedule_matches AS matches

    CROSS JOIN watermark

    WHERE
        matches.created_at >
        watermark.last_processed_at
),


-- ============================================================
-- Recalculate full summaries for affected trips
--
-- We use ALL existing observations for each affected trip,
-- not just the newly inserted observations.
-- ============================================================

affected_trip_summary AS (

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

    FROM affected_trips AS affected

    JOIN vehicle_event_schedule_matches AS matches
      ON affected.feed_checksum = matches.feed_checksum
     AND affected.service_date = matches.service_date
     AND affected.trip_id = matches.trip_id

    JOIN vehicle_events AS events
      ON matches.event_key = events.event_key

    JOIN gtfs_trip_instances AS instances
      ON matches.feed_checksum = instances.feed_checksum
     AND matches.service_date = instances.service_date
     AND matches.trip_id = instances.trip_id

    GROUP BY
        matches.feed_checksum,
        matches.service_date,
        matches.trip_id,
        instances.route_id,
        instances.direction_id
)


-- ============================================================
-- Upsert affected trip summaries
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
-- Advance watermark
-- ============================================================

UPDATE pipeline_watermarks

SET
    last_processed_at = COALESCE(
        (
            SELECT MAX(created_at)
            FROM vehicle_event_schedule_matches
        ),
        last_processed_at
    ),

    updated_at = NOW()

WHERE transformation_name =
    'build_trip_realtime_summary';