-- ============================================================
-- Incremental realtime stop performance
--
-- Recalculate scheduled-vs-observed performance only for trips
-- affected by newly created realtime schedule matches.
-- ============================================================


CREATE TABLE IF NOT EXISTS pipeline_watermarks (

    transformation_name TEXT PRIMARY KEY,

    last_processed_at TIMESTAMPTZ NOT NULL,

    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- ------------------------------------------------------------
-- Bootstrap watermark
--
-- Existing performance data was created by the previous
-- full-history transformation, so on first use we begin from
-- the latest already-known schedule match.
-- ------------------------------------------------------------

INSERT INTO pipeline_watermarks (
    transformation_name,
    last_processed_at
)

SELECT
    'build_realtime_stop_performance',

    CASE

        WHEN EXISTS (
            SELECT 1
            FROM realtime_stop_performance
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
-- Find trips affected since the previous performance run
-- ============================================================

WITH watermark AS (

    SELECT last_processed_at

    FROM pipeline_watermarks

    WHERE transformation_name =
        'build_realtime_stop_performance'
),


affected_trips AS (

    SELECT DISTINCT
        m.feed_checksum,
        m.service_date,
        m.trip_id

    FROM vehicle_event_schedule_matches m

    CROSS JOIN watermark w

    WHERE m.created_at > w.last_processed_at
),


affected_stop_performance AS (

    SELECT
        g.feed_checksum,
        g.service_date,
        g.trip_id,
        g.stop_sequence,
        g.stop_id,

        g.route_id,
        g.direction_id,
        r.vehicle_id,

        g.scheduled_arrival_local
            AT TIME ZONE 'America/New_York'
            AS scheduled_arrival,

        g.scheduled_departure_local
            AT TIME ZONE 'America/New_York'
            AS scheduled_departure,

        r.arrival_estimate,
        r.departure_estimate,

        r.arrival_lower_bound,
        r.arrival_upper_bound,

        r.departure_lower_bound,
        r.departure_upper_bound,


        CASE
            WHEN
                r.arrival_estimate IS NOT NULL
                AND g.scheduled_arrival_local IS NOT NULL

            THEN EXTRACT(
                EPOCH FROM (
                    r.arrival_estimate
                    - (
                        g.scheduled_arrival_local
                        AT TIME ZONE 'America/New_York'
                    )
                )
            )
        END AS arrival_deviation_seconds,


        CASE
            WHEN
                r.departure_estimate IS NOT NULL
                AND g.scheduled_departure_local IS NOT NULL

            THEN EXTRACT(
                EPOCH FROM (
                    r.departure_estimate
                    - (
                        g.scheduled_departure_local
                        AT TIME ZONE 'America/New_York'
                    )
                )
            )
        END AS departure_deviation_seconds,


        CASE
            WHEN
                r.arrival_lower_bound IS NOT NULL
                AND g.scheduled_arrival_local IS NOT NULL

            THEN EXTRACT(
                EPOCH FROM (
                    r.arrival_lower_bound
                    - (
                        g.scheduled_arrival_local
                        AT TIME ZONE 'America/New_York'
                    )
                )
            )
        END AS arrival_deviation_lower_seconds,


        CASE
            WHEN
                r.arrival_upper_bound IS NOT NULL
                AND g.scheduled_arrival_local IS NOT NULL

            THEN EXTRACT(
                EPOCH FROM (
                    r.arrival_upper_bound
                    - (
                        g.scheduled_arrival_local
                        AT TIME ZONE 'America/New_York'
                    )
                )
            )
        END AS arrival_deviation_upper_seconds,


        CASE
            WHEN
                r.departure_lower_bound IS NOT NULL
                AND g.scheduled_departure_local IS NOT NULL

            THEN EXTRACT(
                EPOCH FROM (
                    r.departure_lower_bound
                    - (
                        g.scheduled_departure_local
                        AT TIME ZONE 'America/New_York'
                    )
                )
            )
        END AS departure_deviation_lower_seconds,


        CASE
            WHEN
                r.departure_upper_bound IS NOT NULL
                AND g.scheduled_departure_local IS NOT NULL

            THEN EXTRACT(
                EPOCH FROM (
                    r.departure_upper_bound
                    - (
                        g.scheduled_departure_local
                        AT TIME ZONE 'America/New_York'
                    )
                )
            )
        END AS departure_deviation_upper_seconds,


        r.arrival_uncertainty_seconds,
        r.departure_uncertainty_seconds,

        r.inference_quality


    FROM affected_trips a

    JOIN realtime_stop_events r
      ON a.feed_checksum = r.feed_checksum
     AND a.service_date = r.service_date
     AND a.trip_id = r.trip_id

    JOIN gtfs_stop_instances g
      ON g.feed_checksum = r.feed_checksum
     AND g.service_date = r.service_date
     AND g.trip_id = r.trip_id
     AND g.stop_sequence = r.stop_sequence
     AND g.stop_id = r.stop_id
)


-- ============================================================
-- Upsert only affected stop-performance rows
-- ============================================================

INSERT INTO realtime_stop_performance (

    feed_checksum,
    service_date,
    trip_id,
    stop_sequence,
    stop_id,

    route_id,
    direction_id,
    vehicle_id,

    scheduled_arrival,
    scheduled_departure,

    arrival_estimate,
    departure_estimate,

    arrival_lower_bound,
    arrival_upper_bound,

    departure_lower_bound,
    departure_upper_bound,

    arrival_deviation_seconds,
    departure_deviation_seconds,

    arrival_deviation_lower_seconds,
    arrival_deviation_upper_seconds,

    departure_deviation_lower_seconds,
    departure_deviation_upper_seconds,

    arrival_uncertainty_seconds,
    departure_uncertainty_seconds,

    inference_quality
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
    scheduled_departure,

    arrival_estimate,
    departure_estimate,

    arrival_lower_bound,
    arrival_upper_bound,

    departure_lower_bound,
    departure_upper_bound,

    arrival_deviation_seconds,
    departure_deviation_seconds,

    arrival_deviation_lower_seconds,
    arrival_deviation_upper_seconds,

    departure_deviation_lower_seconds,
    departure_deviation_upper_seconds,

    arrival_uncertainty_seconds,
    departure_uncertainty_seconds,

    inference_quality

FROM affected_stop_performance


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

    scheduled_departure =
        EXCLUDED.scheduled_departure,

    arrival_estimate =
        EXCLUDED.arrival_estimate,

    departure_estimate =
        EXCLUDED.departure_estimate,

    arrival_lower_bound =
        EXCLUDED.arrival_lower_bound,

    arrival_upper_bound =
        EXCLUDED.arrival_upper_bound,

    departure_lower_bound =
        EXCLUDED.departure_lower_bound,

    departure_upper_bound =
        EXCLUDED.departure_upper_bound,

    arrival_deviation_seconds =
        EXCLUDED.arrival_deviation_seconds,

    departure_deviation_seconds =
        EXCLUDED.departure_deviation_seconds,

    arrival_deviation_lower_seconds =
        EXCLUDED.arrival_deviation_lower_seconds,

    arrival_deviation_upper_seconds =
        EXCLUDED.arrival_deviation_upper_seconds,

    departure_deviation_lower_seconds =
        EXCLUDED.departure_deviation_lower_seconds,

    departure_deviation_upper_seconds =
        EXCLUDED.departure_deviation_upper_seconds,

    arrival_uncertainty_seconds =
        EXCLUDED.arrival_uncertainty_seconds,

    departure_uncertainty_seconds =
        EXCLUDED.departure_uncertainty_seconds,

    inference_quality =
        EXCLUDED.inference_quality,

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
    'build_realtime_stop_performance';