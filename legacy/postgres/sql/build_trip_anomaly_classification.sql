CREATE OR REPLACE VIEW trip_anomaly_classification AS

SELECT
    t.*,

    CASE

        -- Not enough of the trip was observed to make
        -- a strong trip-level conclusion.
        WHEN observed_stop_count < 5
            THEN 'INSUFFICIENT_COVERAGE'


        -- Meets our sustained-anomaly requirements.
        WHEN
            anomalous_stop_count >= 3
            AND anomaly_stop_pct >= 50
        THEN

            CASE

                -- More late anomalies than early anomalies.
                WHEN
                    (
                        certain_late_stop_count
                        + possible_late_stop_count
                    )
                    >
                    (
                        certain_early_stop_count
                        + possible_early_stop_count
                    )
                    THEN 'SUSTAINED_LATE'


                -- More early anomalies than late anomalies.
                WHEN
                    (
                        certain_early_stop_count
                        + possible_early_stop_count
                    )
                    >
                    (
                        certain_late_stop_count
                        + possible_late_stop_count
                    )
                    THEN 'SUSTAINED_EARLY'


                -- Rare case where early and late anomaly
                -- evidence is balanced.
                ELSE 'SUSTAINED_MIXED'

            END


        -- Has anomalous stops, but not enough evidence
        -- to call the whole trip disrupted.
        WHEN anomalous_stop_count > 0
            THEN 'ISOLATED_OR_WEAK'


        ELSE 'NORMAL'

    END AS trip_anomaly_status

FROM trip_anomaly_summary t;