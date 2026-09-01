CREATE OR REPLACE VIEW route_daily_operations AS

SELECT
    service_date,
    route_id,

    COUNT(*) AS observed_trips,

    COUNT(*) FILTER (
        WHERE trip_anomaly_status <> 'INSUFFICIENT_COVERAGE'
    ) AS evaluable_trips,

    COUNT(*) FILTER (
        WHERE trip_anomaly_status = 'NORMAL'
    ) AS normal_trips,

    COUNT(*) FILTER (
        WHERE trip_anomaly_status = 'ISOLATED_OR_WEAK'
    ) AS isolated_or_weak_trips,

    COUNT(*) FILTER (
        WHERE trip_anomaly_status = 'SUSTAINED_LATE'
    ) AS sustained_late_trips,

    COUNT(*) FILTER (
        WHERE trip_anomaly_status = 'SUSTAINED_EARLY'
    ) AS sustained_early_trips,

    COUNT(*) FILTER (
        WHERE trip_anomaly_status = 'INSUFFICIENT_COVERAGE'
    ) AS insufficient_coverage_trips,


    -- Percentage of EVALUABLE trips showing a
    -- sustained operational anomaly.
    ROUND(
        (
            100.0
            * COUNT(*) FILTER (
                WHERE trip_anomaly_status IN (
                    'SUSTAINED_LATE',
                    'SUSTAINED_EARLY'
                )
            )
            /
            NULLIF(
                COUNT(*) FILTER (
                    WHERE trip_anomaly_status
                        <> 'INSUFFICIENT_COVERAGE'
                ),
                0
            )
        )::numeric,
        2
    ) AS sustained_anomaly_pct,


    -- Percentage of observed trips for which we did
    -- not have enough stop-level coverage.
    ROUND(
        (
            100.0
            * COUNT(*) FILTER (
                WHERE trip_anomaly_status =
                    'INSUFFICIENT_COVERAGE'
            )
            /
            NULLIF(COUNT(*), 0)
        )::numeric,
        2
    ) AS insufficient_coverage_pct,


    ROUND(
        percentile_cont(0.50)
            WITHIN GROUP (
                ORDER BY observed_stop_count
            )::numeric,
        1
    ) AS median_observed_stops


FROM trip_anomaly_classification

GROUP BY
    service_date,
    route_id;