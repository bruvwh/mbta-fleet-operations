-- ============================================================
-- Route-hour operational summary
--
-- Grain:
--   service_date
--   + route_id
--   + direction_id
--   + service_hour
--
-- Purpose:
--   summarize realtime stop-level performance and anomaly
--   signals into operational time windows.
-- ============================================================


CREATE OR REPLACE VIEW route_hour_operations AS

SELECT

    service_date,
    route_id,
    direction_id,
    service_hour,


    -- --------------------------------------------------------
    -- Volume / observability
    -- --------------------------------------------------------

    COUNT(*) AS observed_stop_events,

    COUNT(DISTINCT trip_id) AS observed_trips,


    -- --------------------------------------------------------
    -- Stop anomaly counts
    -- --------------------------------------------------------

    COUNT(*) FILTER (
        WHERE anomaly_status = 'NORMAL'
    ) AS normal_stop_count,

    COUNT(*) FILTER (
        WHERE anomaly_status = 'CERTAIN_LATE'
    ) AS certain_late_stop_count,

    COUNT(*) FILTER (
        WHERE anomaly_status = 'CERTAIN_EARLY'
    ) AS certain_early_stop_count,

    COUNT(*) FILTER (
        WHERE anomaly_status = 'POSSIBLE_LATE'
    ) AS possible_late_stop_count,

    COUNT(*) FILTER (
        WHERE anomaly_status = 'POSSIBLE_EARLY'
    ) AS possible_early_stop_count,

    COUNT(*) FILTER (
        WHERE anomaly_status = 'UNSCORABLE'
    ) AS unscorable_stop_count,


    COUNT(*) FILTER (
        WHERE anomaly_status IN (
            'CERTAIN_LATE',
            'CERTAIN_EARLY',
            'POSSIBLE_LATE',
            'POSSIBLE_EARLY'
        )
    ) AS anomalous_stop_count,


    -- --------------------------------------------------------
    -- Anomaly percentages
    -- --------------------------------------------------------

    ROUND(
        (
            100.0
            * COUNT(*) FILTER (
                WHERE anomaly_status = 'CERTAIN_LATE'
            )
            /
            NULLIF(
                COUNT(*) FILTER (
                    WHERE anomaly_status <> 'UNSCORABLE'
                ),
                0
            )
        )::numeric,
        2
    ) AS certain_late_pct,


    ROUND(
        (
            100.0
            * COUNT(*) FILTER (
                WHERE anomaly_status = 'CERTAIN_EARLY'
            )
            /
            NULLIF(
                COUNT(*) FILTER (
                    WHERE anomaly_status <> 'UNSCORABLE'
                ),
                0
            )
        )::numeric,
        2
    ) AS certain_early_pct,


    ROUND(
        (
            100.0
            * COUNT(*) FILTER (
                WHERE anomaly_status IN (
                    'CERTAIN_LATE',
                    'CERTAIN_EARLY',
                    'POSSIBLE_LATE',
                    'POSSIBLE_EARLY'
                )
            )
            /
            NULLIF(
                COUNT(*) FILTER (
                    WHERE anomaly_status <> 'UNSCORABLE'
                ),
                0
            )
        )::numeric,
        2
    ) AS anomaly_stop_pct,


    -- --------------------------------------------------------
    -- Actual realtime delay behavior
    -- --------------------------------------------------------

    ROUND(
        percentile_cont(0.50)
            WITHIN GROUP (
                ORDER BY arrival_deviation_seconds
            )::numeric,
        1
    ) AS median_arrival_deviation_seconds,


    ROUND(
        percentile_cont(0.90)
            WITHIN GROUP (
                ORDER BY arrival_deviation_seconds
            )::numeric,
        1
    ) AS p90_arrival_deviation_seconds,


    -- --------------------------------------------------------
    -- Historical expectation for those same observations
    -- --------------------------------------------------------

    ROUND(
        percentile_cont(0.50)
            WITHIN GROUP (
                ORDER BY baseline_median_seconds
            )::numeric,
        1
    ) AS historical_median_seconds,


    -- --------------------------------------------------------
    -- Anomaly-score magnitude
    --
    -- ABS is used here because this describes severity,
    -- regardless of whether it is early or late.
    -- --------------------------------------------------------

    ROUND(
        percentile_cont(0.50)
            WITHIN GROUP (
                ORDER BY ABS(anomaly_score)
            )::numeric,
        2
    ) AS median_abs_anomaly_score,


    ROUND(
        MAX(ABS(anomaly_score))::numeric,
        2
    ) AS max_abs_anomaly_score,


    -- --------------------------------------------------------
    -- Realtime arrival inference quality
    -- --------------------------------------------------------

    ROUND(
        percentile_cont(0.50)
            WITHIN GROUP (
                ORDER BY arrival_uncertainty_seconds
            )::numeric,
        1
    ) AS median_arrival_uncertainty_seconds,


    -- --------------------------------------------------------
    -- Historical baseline source
    -- --------------------------------------------------------

    COUNT(*) FILTER (
        WHERE baseline_type = 'PRIMARY'
    ) AS primary_baseline_count,

    COUNT(*) FILTER (
        WHERE baseline_type = 'FALLBACK'
    ) AS fallback_baseline_count,


    ROUND(
        (
            100.0
            * COUNT(*) FILTER (
                WHERE baseline_type = 'FALLBACK'
            )
            / NULLIF(COUNT(*), 0)
        )::numeric,
        2
    ) AS fallback_baseline_pct


FROM realtime_stop_anomaly_scores

GROUP BY
    service_date,
    route_id,
    direction_id,
    service_hour;