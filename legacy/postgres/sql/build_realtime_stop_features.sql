-- ============================================================
-- Incremental realtime stop features
--
-- Only trips affected by newly created schedule matches are
-- recomputed.
--
-- Performance strategy:
--   1. Capture a deterministic processing window.
--   2. ANALYZE the one-row bounds table.
--   3. Materialize affected trips.
--   4. Materialize ALL schedule matches for those trips.
--   5. ANALYZE the small intermediate datasets.
--   6. Join to partitioned vehicle_events_v2 using:
--
--          event_key
--          event_ingestion_timestamp
--
-- This avoids repeatedly scanning millions of historical rows
-- while preserving complete-trip calculations.
-- ============================================================


-- ============================================================
-- 1. Permanent target table
-- ============================================================

CREATE TABLE IF NOT EXISTS realtime_stop_features (

    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    stop_id TEXT NOT NULL,

    route_id TEXT,
    direction_id INTEGER,
    vehicle_id TEXT,

    scheduled_arrival TIMESTAMPTZ,
    arrival_estimate TIMESTAMPTZ,

    arrival_deviation_seconds DOUBLE PRECISION,
    arrival_uncertainty_seconds DOUBLE PRECISION,

    inference_quality TEXT,

    schedule_match_median_difference_seconds
        DOUBLE PRECISION,

    schedule_match_max_difference_seconds
        DOUBLE PRECISION,

    schedule_match_anomaly BOOLEAN,

    service_hour INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,

    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        feed_checksum,
        service_date,
        trip_id,
        stop_sequence,
        stop_id
    )
);


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

    'build_realtime_stop_features',

    CASE

        WHEN EXISTS (
            SELECT 1
            FROM realtime_stop_features
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
-- 3. Capture this transformation's processing window
--
-- We capture an upper bound before processing begins.
--
-- If new schedule matches arrive during this transformation,
-- they remain available for the next run instead of being
-- skipped when the watermark advances.
--
-- ANALYZE is important even though this temporary table
-- contains only one row. Without statistics PostgreSQL may
-- badly overestimate its size and choose an inefficient plan
-- against the multi-million-row schedule-match table.
-- ============================================================

DROP TABLE IF EXISTS
    temp_realtime_stop_feature_bounds;


CREATE TEMP TABLE
    temp_realtime_stop_feature_bounds
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
        'build_realtime_stop_features';


ANALYZE
    temp_realtime_stop_feature_bounds;


-- ============================================================
-- 4. Materialize affected trips
-- ============================================================

DROP TABLE IF EXISTS
    temp_realtime_stop_feature_affected_trips;


CREATE TEMP TABLE
    temp_realtime_stop_feature_affected_trips
ON COMMIT DROP
AS

SELECT DISTINCT

    matches.feed_checksum,

    matches.service_date,

    matches.trip_id

FROM vehicle_event_schedule_matches
    AS matches

CROSS JOIN
    temp_realtime_stop_feature_bounds
    AS bounds

WHERE

    matches.created_at
        > bounds.last_processed_at

    AND matches.created_at
        <= bounds.batch_upper_bound;


CREATE UNIQUE INDEX
    temp_realtime_stop_feature_affected_trips_pkey

ON temp_realtime_stop_feature_affected_trips (
    feed_checksum,
    service_date,
    trip_id
);


ANALYZE
    temp_realtime_stop_feature_affected_trips;


-- ============================================================
-- 5. Materialize ALL schedule matches for affected trips
--
-- A newly created observation makes its trip affected, but
-- schedule-match statistics are recomputed from the complete
-- set of observations currently available for that trip.
--
-- event_ingestion_timestamp is retained because it is the
-- partition key required to locate the corresponding row in
-- vehicle_events_v2.
-- ============================================================

DROP TABLE IF EXISTS
    temp_realtime_stop_feature_matches;


CREATE TEMP TABLE
    temp_realtime_stop_feature_matches
ON COMMIT DROP
AS

SELECT

    matches.event_key,

    matches.event_ingestion_timestamp,

    matches.feed_checksum,

    matches.service_date,

    matches.trip_id,

    matches.stop_sequence,

    matches.difference_seconds

FROM temp_realtime_stop_feature_affected_trips
    AS affected

JOIN vehicle_event_schedule_matches
    AS matches

  ON affected.feed_checksum
        = matches.feed_checksum

 AND affected.service_date
        = matches.service_date

 AND affected.trip_id
        = matches.trip_id

WHERE
    matches.stop_sequence
        IS NOT NULL;


CREATE UNIQUE INDEX
    temp_realtime_stop_feature_matches_event_idx

ON temp_realtime_stop_feature_matches (
    event_key,
    event_ingestion_timestamp
);


CREATE INDEX
    temp_realtime_stop_feature_matches_trip_stop_idx

ON temp_realtime_stop_feature_matches (
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence
);


ANALYZE
    temp_realtime_stop_feature_matches;


-- ============================================================
-- 6. Recompute schedule-match features for affected trips
--
-- vehicle_events_v2 is partitioned by ingestion_timestamp.
--
-- Each schedule match now contains the two values required to
-- locate its exact vehicle observation:
--
--     event_key
--     event_ingestion_timestamp
-- ============================================================

DROP TABLE IF EXISTS
    temp_realtime_schedule_match_features;


CREATE TEMP TABLE
    temp_realtime_schedule_match_features
ON COMMIT DROP
AS

SELECT

    matches.feed_checksum,

    matches.service_date,

    matches.trip_id,

    matches.stop_sequence,

    events.stop_id,


    percentile_cont(0.50)
        WITHIN GROUP (
            ORDER BY
                matches.difference_seconds
        )::DOUBLE PRECISION
        AS median_difference_seconds,


    MAX(
        matches.difference_seconds
    )::DOUBLE PRECISION
        AS max_difference_seconds


FROM temp_realtime_stop_feature_matches
    AS matches


JOIN vehicle_events_v2
    AS events

  ON events.event_key
        = matches.event_key

 AND events.ingestion_timestamp
        = matches.event_ingestion_timestamp


WHERE
    events.stop_id
        IS NOT NULL


GROUP BY

    matches.feed_checksum,

    matches.service_date,

    matches.trip_id,

    matches.stop_sequence,

    events.stop_id;


-- ============================================================
-- 7. Index the small stop-level schedule-match feature result
-- ============================================================

CREATE UNIQUE INDEX
    temp_realtime_schedule_match_features_pkey

ON temp_realtime_schedule_match_features (
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence,
    stop_id
);


ANALYZE
    temp_realtime_schedule_match_features;


-- ============================================================
-- 8. Build and upsert stop features
-- ============================================================

INSERT INTO realtime_stop_features (

    feed_checksum,

    service_date,

    trip_id,

    stop_sequence,

    stop_id,


    route_id,

    direction_id,

    vehicle_id,


    scheduled_arrival,

    arrival_estimate,


    arrival_deviation_seconds,

    arrival_uncertainty_seconds,


    inference_quality,


    schedule_match_median_difference_seconds,

    schedule_match_max_difference_seconds,

    schedule_match_anomaly,


    service_hour,

    day_of_week,

    is_weekend,


    updated_at
)

SELECT

    performance.feed_checksum,

    performance.service_date,

    performance.trip_id,

    performance.stop_sequence,

    performance.stop_id,


    performance.route_id,

    performance.direction_id,

    performance.vehicle_id,


    performance.scheduled_arrival,

    performance.arrival_estimate,


    performance.arrival_deviation_seconds,

    performance.arrival_uncertainty_seconds,


    performance.inference_quality,


    match_features.median_difference_seconds,

    match_features.max_difference_seconds,


    CASE

        WHEN
            match_features.median_difference_seconds
                > 3600

        THEN TRUE

        ELSE FALSE

    END AS schedule_match_anomaly,


    EXTRACT(
        HOUR FROM
            performance.scheduled_arrival
                AT TIME ZONE
                    'America/New_York'
    )::INTEGER
        AS service_hour,


    EXTRACT(
        ISODOW FROM
            performance.service_date
    )::INTEGER
        AS day_of_week,


    CASE

        WHEN EXTRACT(
            ISODOW FROM
                performance.service_date
        ) IN (6, 7)

        THEN TRUE

        ELSE FALSE

    END AS is_weekend,


    NOW()


FROM
    temp_realtime_stop_feature_affected_trips
        AS affected


JOIN realtime_stop_performance
    AS performance

  ON affected.feed_checksum
        = performance.feed_checksum

 AND affected.service_date
        = performance.service_date

 AND affected.trip_id
        = performance.trip_id


LEFT JOIN temp_realtime_schedule_match_features
    AS match_features

  ON performance.feed_checksum
        = match_features.feed_checksum

 AND performance.service_date
        = match_features.service_date

 AND performance.trip_id
        = match_features.trip_id

 AND performance.stop_sequence
        = match_features.stop_sequence

 AND performance.stop_id
        = match_features.stop_id


ON CONFLICT (
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence,
    stop_id
)

DO UPDATE SET

    route_id =
        EXCLUDED.route_id,

    direction_id =
        EXCLUDED.direction_id,

    vehicle_id =
        EXCLUDED.vehicle_id,

    scheduled_arrival =
        EXCLUDED.scheduled_arrival,

    arrival_estimate =
        EXCLUDED.arrival_estimate,

    arrival_deviation_seconds =
        EXCLUDED.arrival_deviation_seconds,

    arrival_uncertainty_seconds =
        EXCLUDED.arrival_uncertainty_seconds,

    inference_quality =
        EXCLUDED.inference_quality,

    schedule_match_median_difference_seconds =
        EXCLUDED
        .schedule_match_median_difference_seconds,

    schedule_match_max_difference_seconds =
        EXCLUDED
        .schedule_match_max_difference_seconds,

    schedule_match_anomaly =
        EXCLUDED.schedule_match_anomaly,

    service_hour =
        EXCLUDED.service_hour,

    day_of_week =
        EXCLUDED.day_of_week,

    is_weekend =
        EXCLUDED.is_weekend,

    updated_at =
        NOW();


-- ============================================================
-- 9. Advance watermark to the CAPTURED upper bound
--
-- Any schedule matches created after batch_upper_bound remain
-- for the next run.
-- ============================================================

UPDATE pipeline_watermarks

SET

    last_processed_at = (

        SELECT
            batch_upper_bound

        FROM
            temp_realtime_stop_feature_bounds
    ),

    updated_at = NOW()

WHERE
    transformation_name =
        'build_realtime_stop_features';