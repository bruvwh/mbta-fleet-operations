-- ============================================================
-- Trip-level anomaly summary
--
-- Grain:
--   one realtime trip per service date
--
-- Purpose:
--   summarize stop-level anomaly signals into trip-level
--   operational behavior.
-- ============================================================


CREATE TABLE IF NOT EXISTS trip_anomaly_summary (

    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,

    route_id TEXT,
    direction_id INTEGER,

    observed_stop_count BIGINT NOT NULL,

    normal_stop_count BIGINT NOT NULL,

    certain_late_stop_count BIGINT NOT NULL,
    certain_early_stop_count BIGINT NOT NULL,

    possible_late_stop_count BIGINT NOT NULL,
    possible_early_stop_count BIGINT NOT NULL,

    anomalous_stop_count BIGINT NOT NULL,

    anomaly_stop_pct DOUBLE PRECISION,

    max_late_anomaly_score DOUBLE PRECISION,
    max_early_anomaly_score DOUBLE PRECISION,

    first_anomalous_stop_sequence INTEGER,
    last_anomalous_stop_sequence INTEGER,

    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        feed_checksum,
        service_date,
        trip_id
    )
);


WITH trip_summary AS (

    SELECT

        feed_checksum,
        service_date,
        trip_id,

        MAX(route_id) AS route_id,
        MAX(direction_id) AS direction_id,

        COUNT(*) AS observed_stop_count,


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
            WHERE anomaly_status <> 'NORMAL'
        ) AS anomalous_stop_count,


        100.0
        * COUNT(*) FILTER (
            WHERE anomaly_status <> 'NORMAL'
        )
        / COUNT(*)
            AS anomaly_stop_pct,


        MAX(anomaly_score) FILTER (
            WHERE anomaly_score > 0
        ) AS max_late_anomaly_score,


        MIN(anomaly_score) FILTER (
            WHERE anomaly_score < 0
        ) AS max_early_anomaly_score,


        MIN(stop_sequence) FILTER (
            WHERE anomaly_status <> 'NORMAL'
        ) AS first_anomalous_stop_sequence,


        MAX(stop_sequence) FILTER (
            WHERE anomaly_status <> 'NORMAL'
        ) AS last_anomalous_stop_sequence


    FROM realtime_stop_anomaly_scores

    GROUP BY
        feed_checksum,
        service_date,
        trip_id
)


INSERT INTO trip_anomaly_summary (

    feed_checksum,
    service_date,
    trip_id,

    route_id,
    direction_id,

    observed_stop_count,

    normal_stop_count,

    certain_late_stop_count,
    certain_early_stop_count,

    possible_late_stop_count,
    possible_early_stop_count,

    anomalous_stop_count,

    anomaly_stop_pct,

    max_late_anomaly_score,
    max_early_anomaly_score,

    first_anomalous_stop_sequence,
    last_anomalous_stop_sequence,

    updated_at
)

SELECT

    feed_checksum,
    service_date,
    trip_id,

    route_id,
    direction_id,

    observed_stop_count,

    normal_stop_count,

    certain_late_stop_count,
    certain_early_stop_count,

    possible_late_stop_count,
    possible_early_stop_count,

    anomalous_stop_count,

    anomaly_stop_pct,

    max_late_anomaly_score,
    max_early_anomaly_score,

    first_anomalous_stop_sequence,
    last_anomalous_stop_sequence,

    NOW()

FROM trip_summary


ON CONFLICT (
    feed_checksum,
    service_date,
    trip_id
)

DO UPDATE SET

    route_id = EXCLUDED.route_id,
    direction_id = EXCLUDED.direction_id,

    observed_stop_count =
        EXCLUDED.observed_stop_count,

    normal_stop_count =
        EXCLUDED.normal_stop_count,

    certain_late_stop_count =
        EXCLUDED.certain_late_stop_count,

    certain_early_stop_count =
        EXCLUDED.certain_early_stop_count,

    possible_late_stop_count =
        EXCLUDED.possible_late_stop_count,

    possible_early_stop_count =
        EXCLUDED.possible_early_stop_count,

    anomalous_stop_count =
        EXCLUDED.anomalous_stop_count,

    anomaly_stop_pct =
        EXCLUDED.anomaly_stop_pct,

    max_late_anomaly_score =
        EXCLUDED.max_late_anomaly_score,

    max_early_anomaly_score =
        EXCLUDED.max_early_anomaly_score,

    first_anomalous_stop_sequence =
        EXCLUDED.first_anomalous_stop_sequence,

    last_anomalous_stop_sequence =
        EXCLUDED.last_anomalous_stop_sequence,

    updated_at = NOW();