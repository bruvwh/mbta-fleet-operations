CREATE OR REPLACE VIEW realtime_baseline_eligible AS

SELECT *
FROM realtime_stop_features

WHERE
    arrival_deviation_seconds IS NOT NULL

    -- We have a reasonably precise inferred arrival.
    AND arrival_uncertainty_seconds IS NOT NULL
    AND arrival_uncertainty_seconds <= 60

    -- Avoid ambiguous stop inference.
    AND inference_quality IN ('COMPLETE', 'PARTIAL')

    -- Don't let obvious schedule-assignment anomalies
    -- define what "normal" service looks like.
    AND schedule_match_anomaly = FALSE;