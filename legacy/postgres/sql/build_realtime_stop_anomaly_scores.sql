-- ============================================================
-- Realtime stop anomaly scoring
--
-- Grain:
--   one realtime trip-stop observation
--
-- Baseline hierarchy:
--
--   PRIMARY:
--     route + direction + stop + service_hour + weekend
--
--   FALLBACK:
--     route + direction + stop + weekend
--
-- Minimum baseline size:
--   30 arrival observations
--
-- This table does NOT delete unusual observations.
-- It scores how unusual each eligible realtime stop is.
-- ============================================================


CREATE TABLE IF NOT EXISTS realtime_stop_anomaly_scores (

    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    stop_id TEXT NOT NULL,

    route_id TEXT,
    direction_id INTEGER,
    vehicle_id TEXT,

    service_hour INTEGER,
    is_weekend BOOLEAN,

    arrival_deviation_seconds DOUBLE PRECISION,
    arrival_uncertainty_seconds DOUBLE PRECISION,

    arrival_deviation_lower_seconds DOUBLE PRECISION,
    arrival_deviation_upper_seconds DOUBLE PRECISION,

    baseline_type TEXT,
    baseline_count BIGINT,

    baseline_median_seconds DOUBLE PRECISION,
    baseline_p25_seconds DOUBLE PRECISION,
    baseline_p75_seconds DOUBLE PRECISION,
    baseline_iqr_seconds DOUBLE PRECISION,

    lower_anomaly_fence_seconds DOUBLE PRECISION,
    upper_anomaly_fence_seconds DOUBLE PRECISION,

    anomaly_score DOUBLE PRECISION,
    anomaly_status TEXT,

    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        feed_checksum,
        service_date,
        trip_id,
        stop_sequence,
        stop_id
    )
);


-- ============================================================
-- 1. Join realtime observations to both possible baselines
-- ============================================================

WITH baseline_join AS (

    SELECT

        r.*,


        -- Choose PRIMARY whenever enough historical
        -- observations exist.

        CASE
            WHEN b.arrival_count >= 30
                THEN 'PRIMARY'

            WHEN fb.arrival_count >= 30
                THEN 'FALLBACK'

            ELSE NULL
        END AS selected_baseline_type,


        CASE
            WHEN b.arrival_count >= 30
                THEN b.arrival_count
            ELSE fb.arrival_count
        END AS selected_count,


        CASE
            WHEN b.arrival_count >= 30
                THEN b.arrival_median_seconds
            ELSE fb.arrival_median_seconds
        END AS selected_median,


        CASE
            WHEN b.arrival_count >= 30
                THEN b.arrival_p25_seconds
            ELSE fb.arrival_p25_seconds
        END AS selected_p25,


        CASE
            WHEN b.arrival_count >= 30
                THEN b.arrival_p75_seconds
            ELSE fb.arrival_p75_seconds
        END AS selected_p75


    FROM realtime_baseline_eligible r


    LEFT JOIN historical_stop_baselines b
      ON r.route_id = b.route_id
     AND r.direction_id = b.direction_id
     AND r.stop_id = b.stop_id
     AND r.service_hour = b.service_hour
     AND r.is_weekend = b.is_weekend


    LEFT JOIN historical_stop_baselines_fallback fb
      ON r.route_id = fb.route_id
     AND r.direction_id = fb.direction_id
     AND r.stop_id = fb.stop_id
     AND r.is_weekend = fb.is_weekend


    -- LAMP historical baseline currently covers subway.
    WHERE r.route_id IN (
        'Red',
        'Orange',
        'Blue',
        'Green-B',
        'Green-C',
        'Green-D',
        'Green-E',
        'Mattapan'
    )
),


-- ============================================================
-- 2. Calculate baseline ranges and realtime uncertainty range
-- ============================================================

baseline_features AS (

    SELECT

        *,

        selected_p75 - selected_p25
            AS selected_iqr,


        selected_p25
        - 1.5 * (selected_p75 - selected_p25)
            AS lower_fence,


        selected_p75
        + 1.5 * (selected_p75 - selected_p25)
            AS upper_fence,


        -- Our arrival estimate is the upper edge of the
        -- inferred arrival interval.
        --
        -- Therefore:
        --
        -- lower possible deviation =
        -- point estimate - uncertainty
        --
        -- upper possible deviation =
        -- point estimate

        arrival_deviation_seconds
        - arrival_uncertainty_seconds
            AS deviation_lower,


        arrival_deviation_seconds
            AS deviation_upper


    FROM baseline_join

    WHERE selected_baseline_type IS NOT NULL
),


-- ============================================================
-- 3. Calculate anomaly score and uncertainty-aware status
-- ============================================================

scored AS (

    SELECT

        *,


        -- ----------------------------------------------------
        -- Signed robust anomaly score
        --
        -- 0:
        --   inside historical IQR
        --
        -- positive:
        --   unusually late
        --
        -- negative:
        --   unusually early
        --
        -- magnitude:
        --   distance outside IQR measured in IQR units
        -- ----------------------------------------------------

        CASE

            WHEN selected_iqr IS NULL
                 OR selected_iqr <= 0
                THEN NULL


            WHEN arrival_deviation_seconds > selected_p75
                THEN
                    (
                        arrival_deviation_seconds
                        - selected_p75
                    )
                    / selected_iqr


            WHEN arrival_deviation_seconds < selected_p25
                THEN
                    -1.0
                    * (
                        selected_p25
                        - arrival_deviation_seconds
                    )
                    / selected_iqr


            ELSE 0

        END AS calculated_anomaly_score,


        -- ----------------------------------------------------
        -- Uncertainty-aware anomaly classification
        -- ----------------------------------------------------

        CASE

            WHEN selected_iqr IS NULL
                 OR selected_iqr <= 0
                THEN 'UNSCORABLE'


            -- Entire inferred arrival interval lies
            -- above the upper anomaly fence.
            WHEN deviation_lower > upper_fence
                THEN 'CERTAIN_LATE'


            -- Entire inferred arrival interval lies
            -- below the lower anomaly fence.
            WHEN deviation_upper < lower_fence
                THEN 'CERTAIN_EARLY'


            -- Point estimate crosses late fence,
            -- but uncertainty interval overlaps fence.
            WHEN deviation_upper > upper_fence
                 AND deviation_lower <= upper_fence
                THEN 'POSSIBLE_LATE'


            -- Part of uncertainty interval crosses early fence.
            WHEN deviation_lower < lower_fence
                 AND deviation_upper >= lower_fence
                THEN 'POSSIBLE_EARLY'


            ELSE 'NORMAL'

        END AS calculated_anomaly_status


    FROM baseline_features
)


-- ============================================================
-- 4. Store scores
-- ============================================================

INSERT INTO realtime_stop_anomaly_scores (

    feed_checksum,
    service_date,
    trip_id,
    stop_sequence,
    stop_id,

    route_id,
    direction_id,
    vehicle_id,

    service_hour,
    is_weekend,

    arrival_deviation_seconds,
    arrival_uncertainty_seconds,

    arrival_deviation_lower_seconds,
    arrival_deviation_upper_seconds,

    baseline_type,
    baseline_count,

    baseline_median_seconds,
    baseline_p25_seconds,
    baseline_p75_seconds,
    baseline_iqr_seconds,

    lower_anomaly_fence_seconds,
    upper_anomaly_fence_seconds,

    anomaly_score,
    anomaly_status,

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

    service_hour,
    is_weekend,

    arrival_deviation_seconds,
    arrival_uncertainty_seconds,

    deviation_lower,
    deviation_upper,

    selected_baseline_type,
    selected_count,

    selected_median,
    selected_p25,
    selected_p75,
    selected_iqr,

    lower_fence,
    upper_fence,

    calculated_anomaly_score,
    calculated_anomaly_status,

    NOW()

FROM scored


ON CONFLICT (
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence,
    stop_id
)

DO UPDATE SET

    route_id = EXCLUDED.route_id,
    direction_id = EXCLUDED.direction_id,
    vehicle_id = EXCLUDED.vehicle_id,

    service_hour = EXCLUDED.service_hour,
    is_weekend = EXCLUDED.is_weekend,

    arrival_deviation_seconds =
        EXCLUDED.arrival_deviation_seconds,

    arrival_uncertainty_seconds =
        EXCLUDED.arrival_uncertainty_seconds,

    arrival_deviation_lower_seconds =
        EXCLUDED.arrival_deviation_lower_seconds,

    arrival_deviation_upper_seconds =
        EXCLUDED.arrival_deviation_upper_seconds,

    baseline_type =
        EXCLUDED.baseline_type,

    baseline_count =
        EXCLUDED.baseline_count,

    baseline_median_seconds =
        EXCLUDED.baseline_median_seconds,

    baseline_p25_seconds =
        EXCLUDED.baseline_p25_seconds,

    baseline_p75_seconds =
        EXCLUDED.baseline_p75_seconds,

    baseline_iqr_seconds =
        EXCLUDED.baseline_iqr_seconds,

    lower_anomaly_fence_seconds =
        EXCLUDED.lower_anomaly_fence_seconds,

    upper_anomaly_fence_seconds =
        EXCLUDED.upper_anomaly_fence_seconds,

    anomaly_score =
        EXCLUDED.anomaly_score,

    anomaly_status =
        EXCLUDED.anomaly_status,

    updated_at = NOW();