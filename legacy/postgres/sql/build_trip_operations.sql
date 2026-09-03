-- ============================================================
-- Incremental trip operations
--
-- 1. Insert scheduled trips that do not exist yet.
-- 2. Recalculate realtime fields only for affected trips.
-- ============================================================


CREATE TABLE IF NOT EXISTS pipeline_watermarks (

    transformation_name TEXT PRIMARY KEY,

    last_processed_at TIMESTAMPTZ NOT NULL,

    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- ------------------------------------------------------------
-- Bootstrap watermark BEFORE inserting missing scheduled trips.
--
-- On an existing database, historical realtime data has already
-- been processed.
--
-- On a fresh database, start from -infinity so all existing
-- realtime observations are incorporated.
-- ------------------------------------------------------------

INSERT INTO pipeline_watermarks (
    transformation_name,
    last_processed_at
)

SELECT
    'build_trip_operations',

    CASE

        WHEN EXISTS (
            SELECT 1
            FROM trip_operations
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
-- 1. Insert newly scheduled trips
--
-- These trips initially have realtime_observed = FALSE.
-- If realtime data exists for them, the next section updates
-- them.
-- ============================================================

INSERT INTO trip_operations (

    feed_checksum,
    service_date,
    trip_id,
    route_id,
    direction_id,

    scheduled_start_local,
    scheduled_end_local,
    scheduled_stop_count,

    realtime_observed,

    first_observation,
    last_observation,

    observation_count,
    distinct_vehicle_count,

    first_stop_sequence,
    last_stop_sequence
)

SELECT

    instances.feed_checksum,
    instances.service_date,
    instances.trip_id,
    instances.route_id,
    instances.direction_id,

    schedule.scheduled_start_local,
    schedule.scheduled_end_local,
    schedule.scheduled_stop_count,

    FALSE AS realtime_observed,

    NULL AS first_observation,
    NULL AS last_observation,

    0 AS observation_count,
    0 AS distinct_vehicle_count,

    NULL AS first_stop_sequence,
    NULL AS last_stop_sequence

FROM gtfs_trip_instances AS instances

JOIN gtfs_trip_schedule_summary AS schedule
  ON instances.feed_checksum = schedule.feed_checksum
 AND instances.service_date = schedule.service_date
 AND instances.trip_id = schedule.trip_id

WHERE NOT EXISTS (

    SELECT 1

    FROM trip_operations AS existing

    WHERE
        existing.feed_checksum =
            instances.feed_checksum

        AND existing.service_date =
            instances.service_date

        AND existing.trip_id =
            instances.trip_id
)

ON CONFLICT (
    feed_checksum,
    service_date,
    trip_id
)

DO NOTHING;


-- ============================================================
-- 2. Identify trips affected by new realtime observations
-- ============================================================

WITH watermark AS (

    SELECT
        last_processed_at

    FROM pipeline_watermarks

    WHERE transformation_name =
        'build_trip_operations'
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


affected_trip_operations AS (

    SELECT

        instances.feed_checksum,
        instances.service_date,
        instances.trip_id,
        instances.route_id,
        instances.direction_id,

        schedule.scheduled_start_local,
        schedule.scheduled_end_local,
        schedule.scheduled_stop_count,

        TRUE AS realtime_observed,

        realtime.first_observation,
        realtime.last_observation,

        COALESCE(
            realtime.observation_count,
            0
        ) AS observation_count,

        COALESCE(
            realtime.distinct_vehicle_count,
            0
        ) AS distinct_vehicle_count,

        realtime.first_stop_sequence,
        realtime.last_stop_sequence

    FROM affected_trips AS affected

    JOIN gtfs_trip_instances AS instances
      ON affected.feed_checksum =
            instances.feed_checksum
     AND affected.service_date =
            instances.service_date
     AND affected.trip_id =
            instances.trip_id

    JOIN gtfs_trip_schedule_summary AS schedule
      ON instances.feed_checksum =
            schedule.feed_checksum
     AND instances.service_date =
            schedule.service_date
     AND instances.trip_id =
            schedule.trip_id

    JOIN trip_realtime_summary AS realtime
      ON instances.feed_checksum =
            realtime.feed_checksum
     AND instances.service_date =
            realtime.service_date
     AND instances.trip_id =
            realtime.trip_id
)


-- ============================================================
-- 3. Update only affected realtime trips
-- ============================================================

INSERT INTO trip_operations (

    feed_checksum,
    service_date,
    trip_id,
    route_id,
    direction_id,

    scheduled_start_local,
    scheduled_end_local,
    scheduled_stop_count,

    realtime_observed,

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

    scheduled_start_local,
    scheduled_end_local,
    scheduled_stop_count,

    realtime_observed,

    first_observation,
    last_observation,

    observation_count,
    distinct_vehicle_count,

    first_stop_sequence,
    last_stop_sequence

FROM affected_trip_operations

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

    scheduled_start_local =
        EXCLUDED.scheduled_start_local,

    scheduled_end_local =
        EXCLUDED.scheduled_end_local,

    scheduled_stop_count =
        EXCLUDED.scheduled_stop_count,

    realtime_observed =
        EXCLUDED.realtime_observed,

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
-- 4. Advance watermark
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
    'build_trip_operations';