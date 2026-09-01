-- ============================================================
-- Incremental realtime stop features
--
-- Only rebuild features for trips affected by new realtime
-- schedule matches.
--
-- For an affected trip, ALL of its schedule matches are used
-- when recomputing stop-level match statistics.
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

    schedule_match_median_difference_seconds DOUBLE PRECISION,
    schedule_match_max_difference_seconds DOUBLE PRECISION,
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
-- Bootstrap watermark
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
-- Find affected trips
-- ============================================================

WITH watermark AS (

    SELECT last_processed_at

    FROM pipeline_watermarks

    WHERE transformation_name =
        'build_realtime_stop_features'
),


affected_trips AS (

    SELECT DISTINCT
        m.feed_checksum,
        m.service_date,
        m.trip_id

    FROM vehicle_event_schedule_matches AS m

    CROSS JOIN watermark AS w

    WHERE
        m.created_at > w.last_processed_at
),


-- ============================================================
-- Recalculate schedule-match statistics only for affected trips
--
-- Important:
-- We use ALL matches belonging to each affected trip, not only
-- the newly created matches.
-- ============================================================

schedule_match_features AS (

    SELECT
        m.feed_checksum,
        m.service_date,
        m.trip_id,
        m.stop_sequence,
        v.stop_id,

        percentile_cont(0.50)
            WITHIN GROUP (
                ORDER BY m.difference_seconds
            ) AS median_difference_seconds,

        MAX(m.difference_seconds)
            AS max_difference_seconds

    FROM affected_trips AS affected

    JOIN vehicle_event_schedule_matches AS m
      ON affected.feed_checksum = m.feed_checksum
     AND affected.service_date = m.service_date
     AND affected.trip_id = m.trip_id

    JOIN vehicle_events AS v
      ON m.event_key = v.event_key

    WHERE
        m.stop_sequence IS NOT NULL
        AND v.stop_id IS NOT NULL

    GROUP BY
        m.feed_checksum,
        m.service_date,
        m.trip_id,
        m.stop_sequence,
        v.stop_id
),


-- ============================================================
-- Build features only for affected trips
-- ============================================================

affected_stop_features AS (

    SELECT

        p.feed_checksum,
        p.service_date,
        p.trip_id,
        p.stop_sequence,
        p.stop_id,

        p.route_id,
        p.direction_id,
        p.vehicle_id,

        p.scheduled_arrival,
        p.arrival_estimate,

        p.arrival_deviation_seconds,
        p.arrival_uncertainty_seconds,

        p.inference_quality,

        s.median_difference_seconds,
        s.max_difference_seconds,

        CASE
            WHEN s.median_difference_seconds > 3600
                THEN TRUE
            ELSE FALSE
        END AS schedule_match_anomaly,

        EXTRACT(
            HOUR FROM
            p.scheduled_arrival
                AT TIME ZONE 'America/New_York'
        )::INTEGER AS service_hour,

        EXTRACT(
            ISODOW FROM p.service_date
        )::INTEGER AS day_of_week,

        CASE
            WHEN EXTRACT(
                ISODOW FROM p.service_date
            ) IN (6, 7)
                THEN TRUE
            ELSE FALSE
        END AS is_weekend

    FROM affected_trips AS affected

    JOIN realtime_stop_performance AS p
      ON affected.feed_checksum = p.feed_checksum
     AND affected.service_date = p.service_date
     AND affected.trip_id = p.trip_id

    LEFT JOIN schedule_match_features AS s
      ON p.feed_checksum = s.feed_checksum
     AND p.service_date = s.service_date
     AND p.trip_id = s.trip_id
     AND p.stop_sequence = s.stop_sequence
     AND p.stop_id = s.stop_id
)


-- ============================================================
-- Upsert affected stop features
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

    median_difference_seconds,
    max_difference_seconds,
    schedule_match_anomaly,

    service_hour,
    day_of_week,
    is_weekend,

    NOW()

FROM affected_stop_features


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
        EXCLUDED.schedule_match_median_difference_seconds,

    schedule_match_max_difference_seconds =
        EXCLUDED.schedule_match_max_difference_seconds,

    schedule_match_anomaly =
        EXCLUDED.schedule_match_anomaly,

    service_hour =
        EXCLUDED.service_hour,

    day_of_week =
        EXCLUDED.day_of_week,

    is_weekend =
        EXCLUDED.is_weekend,

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
    'build_realtime_stop_features';