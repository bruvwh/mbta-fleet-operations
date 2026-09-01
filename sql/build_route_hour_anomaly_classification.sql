CREATE OR REPLACE VIEW route_hour_anomaly_classification AS

SELECT
    r.*,

    CASE

        -- Not enough observations to confidently evaluate
        -- this route-direction-hour window.
        WHEN observed_stop_events < 10
             OR observed_trips < 3
            THEN 'INSUFFICIENT_COVERAGE'


        -- Majority of observations are anomalous.
        WHEN anomaly_stop_pct >= 50
        THEN

            CASE

                WHEN certain_late_pct > certain_early_pct
                    THEN 'ANOMALOUS_LATE'

                WHEN certain_early_pct > certain_late_pct
                    THEN 'ANOMALOUS_EARLY'

                ELSE 'ANOMALOUS_MIXED'

            END


        -- Some anomaly activity, but not enough to classify
        -- the whole operating window as disrupted.
        WHEN anomaly_stop_pct > 0
            THEN 'ELEVATED'


        ELSE 'NORMAL'

    END AS route_hour_status

FROM route_hour_operations r;