CREATE OR REPLACE VIEW historical_baseline_eligible AS

SELECT *
FROM historical_stop_features

WHERE
    trip_type = 'PLANNED'
    AND arrival_deviation_seconds IS NOT NULL;