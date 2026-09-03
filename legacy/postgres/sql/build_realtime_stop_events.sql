-- ============================================================
-- Incremental realtime stop-event inference
--
-- Instead of rebuilding stop events for the entire realtime
-- history, only trips affected by NEW schedule matches are
-- recomputed.
--
-- IMPORTANT:
-- Once a trip is affected, ALL matched observations for that
-- trip are included so LAG / LEAD calculations remain correct.
--
-- Performance strategy:
--   1. Capture a deterministic processing window.
--   2. ANALYZE the one-row bounds table so PostgreSQL knows its
--      true cardinality before planning the affected-trip query.
--   3. Materialize affected trips.
--   4. ANALYZE the actual affected-trip set.
--   5. Materialize all schedule matches for those trips.
--   6. ANALYZE those matches.
--   7. Join directly to the partitioned vehicle_events_v2
--      table using:
--
--          event_key
--          event_ingestion_timestamp
--
-- This gives PostgreSQL accurate incremental cardinalities
-- before accessing the large realtime history.
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
-- 2. Bootstrap transformation watermark
--
-- If realtime_stop_events already contains data, existing
-- schedule matches have already been processed.
--
-- Fresh installations begin at -infinity.
-- ============================================================

INSERT INTO pipeline_watermarks (
    transformation_name,
    last_processed_at
)

SELECT

    'build_realtime_stop_events',

    CASE

        WHEN EXISTS (
            SELECT 1
            FROM realtime_stop_events
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
-- The upper bound is captured before expensive processing
-- begins.
--
-- Schedule matches created after this upper bound remain
-- pending for the next pipeline run.
--
-- ANALYZE is important here even though this table contains
-- only one row. Without statistics PostgreSQL can badly
-- overestimate its size and choose a plan that scans nearly
-- the entire schedule-match history.
-- ============================================================

DROP TABLE IF EXISTS
    temp_stop_event_bounds;


CREATE TEMP TABLE
    temp_stop_event_bounds
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
        'build_realtime_stop_events';


ANALYZE
    temp_stop_event_bounds;


-- ============================================================
-- 4. Materialize trips affected by NEW schedule matches
--
-- Materializing this set gives PostgreSQL its actual size
-- before the larger historical joins are planned.
-- ============================================================

DROP TABLE IF EXISTS
    temp_stop_affected_trips;


CREATE TEMP TABLE
    temp_stop_affected_trips
ON COMMIT DROP
AS

SELECT DISTINCT

    matches.feed_checksum,

    matches.service_date,

    matches.trip_id

FROM vehicle_event_schedule_matches
    AS matches

CROSS JOIN temp_stop_event_bounds
    AS bounds

WHERE

    matches.created_at
        > bounds.last_processed_at

    AND matches.created_at
        <= bounds.batch_upper_bound;


CREATE UNIQUE INDEX
    temp_stop_affected_trips_pkey

ON temp_stop_affected_trips (
    feed_checksum,
    service_date,
    trip_id
);


ANALYZE
    temp_stop_affected_trips;


-- ============================================================
-- 5. Materialize ALL schedule matches for affected trips
--
-- We intentionally retrieve the complete affected trip.
--
-- A new observation can change:
--   - previous observation
--   - next observation
--   - first STOPPED_AT
--   - last STOPPED_AT
--   - stop-period counts
--
-- Therefore calculations must use all observations belonging
-- to each affected trip.
-- ============================================================

DROP TABLE IF EXISTS
    temp_stop_schedule_matches;


CREATE TEMP TABLE
    temp_stop_schedule_matches
ON COMMIT DROP
AS

SELECT

    matches.event_key,

    matches.event_ingestion_timestamp,

    matches.feed_checksum,

    matches.service_date,

    matches.trip_id,

    matches.scheduled_local_time

FROM temp_stop_affected_trips
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
    temp_stop_schedule_matches_event_idx

ON temp_stop_schedule_matches (
    event_key,
    event_ingestion_timestamp
);


CREATE INDEX
    temp_stop_schedule_matches_trip_idx

ON temp_stop_schedule_matches (
    feed_checksum,
    service_date,
    trip_id
);


ANALYZE
    temp_stop_schedule_matches;


-- ============================================================
-- 6. Retrieve partitioned vehicle observations
--
-- Each schedule match contains:
--
--     event_key
--     event_ingestion_timestamp
--
-- Together these identify the exact row in vehicle_events_v2.
-- ============================================================

WITH matched_events AS (

    SELECT

        vehicles.event_key,

        vehicles.vehicle_id,

        vehicles.route_id,

        vehicles.direction_id,

        vehicles.stop_id,

        vehicles.current_stop_sequence
            AS stop_sequence,

        vehicles.current_status,

        vehicles.vehicle_timestamp,

        matches.feed_checksum,

        matches.service_date,

        matches.trip_id,

        matches.scheduled_local_time

    FROM temp_stop_schedule_matches
        AS matches

    JOIN vehicle_events_v2
        AS vehicles

      ON vehicles.event_key
            = matches.event_key

     AND vehicles.ingestion_timestamp
            = matches.event_ingestion_timestamp

    WHERE

        vehicles.vehicle_timestamp
            IS NOT NULL

        AND vehicles.stop_id
            IS NOT NULL

        AND vehicles.current_stop_sequence
            IS NOT NULL
),


-- ============================================================
-- 7. Order observations within each vehicle/trip
-- ============================================================

ordered_events AS (

    SELECT

        *,

        LAG(vehicle_timestamp) OVER (

            PARTITION BY
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            ORDER BY
                vehicle_timestamp,
                event_key

        ) AS previous_timestamp,


        LAG(current_status) OVER (

            PARTITION BY
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            ORDER BY
                vehicle_timestamp,
                event_key

        ) AS previous_status,


        LAG(stop_sequence) OVER (

            PARTITION BY
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            ORDER BY
                vehicle_timestamp,
                event_key

        ) AS previous_stop_sequence,


        LAG(stop_id) OVER (

            PARTITION BY
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            ORDER BY
                vehicle_timestamp,
                event_key

        ) AS previous_stop_id,


        LEAD(vehicle_timestamp) OVER (

            PARTITION BY
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            ORDER BY
                vehicle_timestamp,
                event_key

        ) AS next_timestamp,


        LEAD(current_status) OVER (

            PARTITION BY
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            ORDER BY
                vehicle_timestamp,
                event_key

        ) AS next_status,


        LEAD(stop_sequence) OVER (

            PARTITION BY
                feed_checksum,
                service_date,
                trip_id,
                vehicle_id

            ORDER BY
                vehicle_timestamp,
                event_key

        ) AS next_stop_sequence

    FROM matched_events
),


-- ============================================================
-- 8. Rank STOPPED_AT observations for each trip-stop
-- ============================================================

ranked_stopped_events AS (

    SELECT

        *,

        ROW_NUMBER() OVER (

            PARTITION BY
                feed_checksum,
                service_date,
                trip_id,
                stop_sequence,
                stop_id

            ORDER BY
                vehicle_timestamp

        ) AS stopped_rank_first,


        ROW_NUMBER() OVER (

            PARTITION BY
                feed_checksum,
                service_date,
                trip_id,
                stop_sequence,
                stop_id

            ORDER BY
                vehicle_timestamp DESC

        ) AS stopped_rank_last

    FROM ordered_events

    WHERE
        current_status = 1
),


-- ============================================================
-- 9. Summarize observations for each trip-stop
-- ============================================================

stop_summary AS (

    SELECT

        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        stop_id,


        MAX(route_id)
            AS route_id,


        MAX(direction_id)
            AS direction_id,


        COUNT(DISTINCT vehicle_id)
            AS vehicle_count,


        CASE

            WHEN COUNT(DISTINCT vehicle_id) = 1

                THEN MAX(vehicle_id)

            ELSE NULL

        END AS vehicle_id,


        MIN(scheduled_local_time)
            AS scheduled_local_time,


        COUNT(*)
            AS total_observation_count,


        COUNT(*) FILTER (

            WHERE current_status = 1

        ) AS stopped_observation_count,


        COUNT(*) FILTER (

            WHERE

                current_status = 1

                AND (

                    previous_status IS NULL

                    OR previous_status <> 1

                    OR previous_stop_sequence
                        <> stop_sequence

                    OR previous_stop_id
                        <> stop_id
                )

        ) AS stopped_period_count

    FROM ordered_events

    GROUP BY

        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        stop_id
),


-- ============================================================
-- 10. Infer arrival bounds
-- ============================================================

first_stopped AS (

    SELECT

        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        stop_id,


        vehicle_timestamp
            AS first_stopped_timestamp,


        CASE

            WHEN

                previous_status IN (0, 2)

                AND previous_stop_sequence
                    = stop_sequence

                AND previous_stop_id
                    = stop_id

            THEN previous_timestamp


            WHEN

                previous_stop_sequence
                    IS NOT NULL

                AND previous_stop_sequence
                    < stop_sequence

            THEN previous_timestamp


            ELSE NULL

        END AS arrival_lower_bound,


        CASE

            WHEN

                previous_status IN (0, 2)

                AND previous_stop_sequence
                    = stop_sequence

                AND previous_stop_id
                    = stop_id

            THEN 'SAME_STOP_TRANSITION'


            WHEN

                previous_stop_sequence
                    IS NOT NULL

                AND previous_stop_sequence
                    < stop_sequence

            THEN 'PREVIOUS_STOP'


            ELSE 'UNBOUNDED'

        END AS arrival_bound_method


    FROM ranked_stopped_events

    WHERE
        stopped_rank_first = 1
),


-- ============================================================
-- 11. Infer departure bounds
-- ============================================================

last_stopped AS (

    SELECT

        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        stop_id,


        vehicle_timestamp
            AS last_stopped_timestamp,


        CASE

            WHEN

                next_status IN (0, 2)

                AND next_stop_sequence
                    >= stop_sequence

            THEN next_timestamp


            ELSE NULL

        END AS departure_upper_bound


    FROM ranked_stopped_events

    WHERE
        stopped_rank_last = 1
),


-- ============================================================
-- 12. Build final inferred stop events
-- ============================================================

final_events AS (

    SELECT

        summary.*,


        first_stop.arrival_lower_bound,


        first_stop.first_stopped_timestamp
            AS arrival_upper_bound,


        first_stop.first_stopped_timestamp
            AS arrival_estimate,


        COALESCE(

            first_stop.arrival_bound_method,

            'NO_STOP_OBSERVATION'

        ) AS arrival_bound_method,


        last_stop.last_stopped_timestamp
            AS departure_lower_bound,


        last_stop.departure_upper_bound,


        last_stop.departure_upper_bound
            AS departure_estimate,


        CASE

            WHEN

                first_stop.arrival_lower_bound
                    IS NOT NULL

                AND first_stop.first_stopped_timestamp
                    IS NOT NULL

            THEN EXTRACT(

                EPOCH FROM (

                    first_stop.first_stopped_timestamp

                    - first_stop.arrival_lower_bound
                )
            )

        END AS arrival_uncertainty_seconds,


        CASE

            WHEN

                last_stop.last_stopped_timestamp
                    IS NOT NULL

                AND last_stop.departure_upper_bound
                    IS NOT NULL

            THEN EXTRACT(

                EPOCH FROM (

                    last_stop.departure_upper_bound

                    - last_stop.last_stopped_timestamp
                )
            )

        END AS departure_uncertainty_seconds,


        CASE

            WHEN summary.vehicle_count > 1

                THEN 'MULTIPLE_VEHICLES'


            WHEN summary.stopped_observation_count = 0

                THEN 'NO_STOP_OBSERVATION'


            WHEN summary.stopped_period_count > 1

                THEN 'MULTIPLE_STOP_PERIODS'


            WHEN

                first_stop.arrival_lower_bound
                    IS NOT NULL

                AND last_stop.departure_upper_bound
                    IS NOT NULL

            THEN 'COMPLETE'


            ELSE 'PARTIAL'

        END AS inference_quality


    FROM stop_summary
        AS summary


    LEFT JOIN first_stopped
        AS first_stop

      USING (
          feed_checksum,
          service_date,
          trip_id,
          stop_sequence,
          stop_id
      )


    LEFT JOIN last_stopped
        AS last_stop

      USING (
          feed_checksum,
          service_date,
          trip_id,
          stop_sequence,
          stop_id
      )
)


-- ============================================================
-- 13. Upsert affected stop events
-- ============================================================

INSERT INTO realtime_stop_events (

    feed_checksum,

    service_date,

    trip_id,

    stop_sequence,

    stop_id,


    route_id,

    direction_id,


    vehicle_id,

    vehicle_count,


    scheduled_local_time,


    arrival_lower_bound,

    arrival_upper_bound,

    arrival_estimate,


    departure_lower_bound,

    departure_upper_bound,

    departure_estimate,


    arrival_uncertainty_seconds,

    arrival_bound_method,

    departure_uncertainty_seconds,


    stopped_observation_count,

    total_observation_count,


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

    vehicle_count,


    scheduled_local_time,


    arrival_lower_bound,

    arrival_upper_bound,

    arrival_estimate,


    departure_lower_bound,

    departure_upper_bound,

    departure_estimate,


    arrival_uncertainty_seconds,

    arrival_bound_method,

    departure_uncertainty_seconds,


    stopped_observation_count,

    total_observation_count,


    inference_quality

FROM final_events


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


    vehicle_count =
        EXCLUDED.vehicle_count,


    scheduled_local_time =
        EXCLUDED.scheduled_local_time,


    arrival_lower_bound =
        EXCLUDED.arrival_lower_bound,


    arrival_upper_bound =
        EXCLUDED.arrival_upper_bound,


    arrival_estimate =
        EXCLUDED.arrival_estimate,


    arrival_bound_method =
        EXCLUDED.arrival_bound_method,


    departure_lower_bound =
        EXCLUDED.departure_lower_bound,


    departure_upper_bound =
        EXCLUDED.departure_upper_bound,


    departure_estimate =
        EXCLUDED.departure_estimate,


    arrival_uncertainty_seconds =
        EXCLUDED.arrival_uncertainty_seconds,


    departure_uncertainty_seconds =
        EXCLUDED.departure_uncertainty_seconds,


    stopped_observation_count =
        EXCLUDED.stopped_observation_count,


    total_observation_count =
        EXCLUDED.total_observation_count,


    inference_quality =
        EXCLUDED.inference_quality,


    updated_at = NOW();


-- ============================================================
-- 14. Advance transformation watermark
--
-- Advance only to the fixed upper bound captured before this
-- transformation began.
--
-- If anything fails, the Python pipeline rolls back both the
-- data changes and watermark change.
-- ============================================================

UPDATE pipeline_watermarks

SET

    last_processed_at = (

        SELECT
            batch_upper_bound

        FROM temp_stop_event_bounds
    ),

    updated_at = NOW()

WHERE
    transformation_name =
        'build_realtime_stop_events';