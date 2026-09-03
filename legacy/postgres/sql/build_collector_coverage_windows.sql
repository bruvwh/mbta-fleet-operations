TRUNCATE TABLE collector_coverage_windows;

WITH snapshot_gaps AS (
    SELECT
        ingestion_timestamp,

        LAG(ingestion_timestamp) OVER (
            ORDER BY ingestion_timestamp
        ) AS previous_timestamp

    FROM processed_files
),

window_boundaries AS (
    SELECT
        ingestion_timestamp,

        CASE
            WHEN previous_timestamp IS NULL THEN 1

            WHEN ingestion_timestamp
                 - previous_timestamp
                 > INTERVAL '90 seconds'
                THEN 1

            ELSE 0
        END AS new_window

    FROM snapshot_gaps
),

window_groups AS (
    SELECT
        ingestion_timestamp,

        SUM(new_window) OVER (
            ORDER BY ingestion_timestamp
        ) AS window_group

    FROM window_boundaries
)

INSERT INTO collector_coverage_windows (
    window_start,
    window_end,
    snapshot_count
)

SELECT
    MIN(ingestion_timestamp),
    MAX(ingestion_timestamp),
    COUNT(*)

FROM window_groups

GROUP BY window_group

ORDER BY MIN(ingestion_timestamp);