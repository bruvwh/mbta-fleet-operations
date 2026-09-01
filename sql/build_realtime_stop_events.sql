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
-- ============================================================


-- ------------------------------------------------------------
-- 1. Track how far this transformation has processed
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline_watermarks (

    transformation_name TEXT PRIMARY KEY,

    last_processed_at TIMESTAMPTZ NOT NULL,

    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- ------------------------------------------------------------
-- Bootstrap the watermark.
--
-- If realtime_stop_events already contains data, we know the
-- existing schedule matches were already processed by the old
-- full-history transformation, so start from the latest match.
--
-- On a completely fresh database, start from -infinity so the
-- first run processes everything.
-- ------------------------------------------------------------

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
-- 2. Find trips affected by newly created schedule matches
-- ============================================================

WITH watermark AS (

    SELECT
        last_processed_at

    FROM pipeline_watermarks

    WHERE transformation_name =
        'build_realtime_stop_events'

),


affected_trips AS (

    SELECT DISTINCT

        m.feed_checksum,
        m.service_date,
        m.trip_id

    FROM vehicle_event_schedule_matches m

    CROSS JOIN watermark w

    WHERE
        m.created_at > w.last_processed_at
),


-- ============================================================
-- 3. Retrieve ALL matched observations for affected trips
--
-- We intentionally do not restrict this to only new events.
--
-- A new event can change:
--   - previous observation
--   - next observation
--   - first STOPPED_AT
--   - last STOPPED_AT
--   - stop-period counts
--
-- Therefore the full affected trip needs to be recomputed.
-- ============================================================

matched_events AS (

    SELECT

        v.event_key,
        v.vehicle_id,
        v.route_id,
        v.direction_id,
        v.stop_id,

        v.current_stop_sequence
            AS stop_sequence,

        v.current_status,
        v.vehicle_timestamp,

        m.feed_checksum,
        m.service_date,
        m.trip_id,
        m.scheduled_local_time

    FROM affected_trips a

    JOIN vehicle_event_schedule_matches m
      ON a.feed_checksum = m.feed_checksum
     AND a.service_date = m.service_date
     AND a.trip_id = m.trip_id

    JOIN vehicle_events v
      ON v.event_key = m.event_key

    WHERE
        v.vehicle_timestamp IS NOT NULL
        AND v.stop_id IS NOT NULL
        AND v.current_stop_sequence IS NOT NULL
),


-- ============================================================
-- 4. Order observations within each vehicle/trip
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
-- 5. Rank STOPPED_AT observations for each trip-stop
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

    WHERE current_status = 1
),


-- ============================================================
-- 6. Summarize observations for each trip-stop
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
-- 7. Infer arrival bounds
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

            -- Best case:
            -- immediately before STOPPED_AT the vehicle was
            -- moving toward this same stop.

            WHEN
                previous_status IN (0, 2)

                AND previous_stop_sequence
                    = stop_sequence

                AND previous_stop_id
                    = stop_id

            THEN previous_timestamp


            -- Fallback:
            -- previous observation belongs to an earlier
            -- stop in this same trip.

            WHEN
                previous_stop_sequence IS NOT NULL

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
                previous_stop_sequence IS NOT NULL

                AND previous_stop_sequence
                    < stop_sequence

            THEN 'PREVIOUS_STOP'


            ELSE 'UNBOUNDED'

        END AS arrival_bound_method


    FROM ranked_stopped_events

    WHERE stopped_rank_first = 1
),


-- ============================================================
-- 8. Infer departure bounds
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

    WHERE stopped_rank_last = 1
),


-- ============================================================
-- 9. Final inferred stop events
-- ============================================================

final_events AS (

    SELECT

        s.*,

        f.arrival_lower_bound,

        f.first_stopped_timestamp
            AS arrival_upper_bound,

        f.first_stopped_timestamp
            AS arrival_estimate,


        COALESCE(
            f.arrival_bound_method,
            'NO_STOP_OBSERVATION'
        ) AS arrival_bound_method,


        l.last_stopped_timestamp
            AS departure_lower_bound,

        l.departure_upper_bound,

        l.departure_upper_bound
            AS departure_estimate,


        CASE

            WHEN
                f.arrival_lower_bound IS NOT NULL

                AND f.first_stopped_timestamp
                    IS NOT NULL

            THEN EXTRACT(
                EPOCH FROM (
                    f.first_stopped_timestamp
                    - f.arrival_lower_bound
                )
            )

        END AS arrival_uncertainty_seconds,


        CASE

            WHEN
                l.last_stopped_timestamp IS NOT NULL

                AND l.departure_upper_bound
                    IS NOT NULL

            THEN EXTRACT(
                EPOCH FROM (
                    l.departure_upper_bound
                    - l.last_stopped_timestamp
                )
            )

        END AS departure_uncertainty_seconds,


        CASE

            WHEN s.vehicle_count > 1
                THEN 'MULTIPLE_VEHICLES'

            WHEN s.stopped_observation_count = 0
                THEN 'NO_STOP_OBSERVATION'

            WHEN s.stopped_period_count > 1
                THEN 'MULTIPLE_STOP_PERIODS'

            WHEN
                f.arrival_lower_bound IS NOT NULL
                AND l.departure_upper_bound IS NOT NULL
                THEN 'COMPLETE'

            ELSE 'PARTIAL'

        END AS inference_quality


    FROM stop_summary s

    LEFT JOIN first_stopped f
      USING (
          feed_checksum,
          service_date,
          trip_id,
          stop_sequence,
          stop_id
      )

    LEFT JOIN last_stopped l
      USING (
          feed_checksum,
          service_date,
          trip_id,
          stop_sequence,
          stop_id
      )
)


-- ============================================================
-- 10. Upsert affected stop events
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
-- 11. Advance transformation watermark
--
-- This runs in the same transaction as the transformation.
-- If the transformation fails, the Python pipeline rolls back
-- and this watermark does not advance.
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
    'build_realtime_stop_events';