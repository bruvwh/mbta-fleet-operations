-- ============================================================
-- Incremental collector coverage for scheduled trips
--
-- Only recalculate service dates affected by newly processed
-- realtime snapshots, plus trips that have never had their
-- coverage calculated.
-- ============================================================


CREATE TABLE IF NOT EXISTS pipeline_watermarks (

    transformation_name TEXT PRIMARY KEY,

    last_processed_at TIMESTAMPTZ NOT NULL,

    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- ------------------------------------------------------------
-- Initialize watermark.
--
-- Starting from -infinity makes the first run safe:
-- it processes all collector dates currently stored.
-- Later runs only process newly added snapshots.
-- ------------------------------------------------------------

INSERT INTO pipeline_watermarks (
    transformation_name,
    last_processed_at
)

VALUES (
    'update_trip_operations_coverage',
    '-infinity'::timestamptz
)

ON CONFLICT (transformation_name)
DO NOTHING;


-- ============================================================
-- Determine which service dates could have changed
-- ============================================================

WITH watermark AS (

    SELECT
        last_processed_at,
        NOW() AS run_cutoff

    FROM pipeline_watermarks

    WHERE transformation_name =
        'update_trip_operations_coverage'
),


new_snapshot_dates AS (

    SELECT DISTINCT

        (
            processed.ingestion_timestamp
            AT TIME ZONE 'America/New_York'
        )::date AS snapshot_date

    FROM processed_files AS processed

    CROSS JOIN watermark

    WHERE
        processed.processed_at >
            watermark.last_processed_at

        AND processed.processed_at <=
            watermark.run_cutoff
),


-- Include +/- 1 day because GTFS service can cross midnight.
affected_service_dates AS (

    SELECT DISTINCT
        snapshot_date + offset_days
            AS service_date

    FROM new_snapshot_dates

    CROSS JOIN (
        VALUES
            (-1),
            (0),
            (1)
    ) AS offsets(offset_days)
),


-- ============================================================
-- Select only trips whose coverage may need recalculation
-- ============================================================

affected_trips AS (

    SELECT
        trips.*

    FROM trip_operations AS trips

    WHERE

        trips.service_date IN (
            SELECT service_date
            FROM affected_service_dates
        )

        -- Also calculate coverage for newly inserted scheduled
        -- trips that have never been processed.
        OR trips.scheduled_duration_seconds IS NULL
        OR trips.collector_coverage_seconds IS NULL
        OR trips.collector_coverage_percent IS NULL
),


-- ============================================================
-- Recalculate collector coverage
-- ============================================================

trip_coverage AS (

    SELECT
        trips.feed_checksum,
        trips.service_date,
        trips.trip_id,

        EXTRACT(
            EPOCH FROM (
                trips.scheduled_end_local
                - trips.scheduled_start_local
            )
        ) AS scheduled_duration_seconds,

        COALESCE(

            SUM(

                EXTRACT(
                    EPOCH FROM (

                        LEAST(
                            trips.scheduled_end_local,

                            windows.window_end
                                AT TIME ZONE
                                'America/New_York'
                        )

                        -

                        GREATEST(
                            trips.scheduled_start_local,

                            windows.window_start
                                AT TIME ZONE
                                'America/New_York'
                        )
                    )
                )

            ) FILTER (

                WHERE
                    windows.window_start
                        AT TIME ZONE
                        'America/New_York'
                        < trips.scheduled_end_local

                AND

                    windows.window_end
                        AT TIME ZONE
                        'America/New_York'
                        > trips.scheduled_start_local
            ),

            0

        ) AS collector_coverage_seconds

    FROM affected_trips AS trips

    LEFT JOIN collector_coverage_windows AS windows

      ON windows.window_start
            AT TIME ZONE 'America/New_York'
            < trips.scheduled_end_local

     AND windows.window_end
            AT TIME ZONE 'America/New_York'
            > trips.scheduled_start_local

    GROUP BY
        trips.feed_checksum,
        trips.service_date,
        trips.trip_id,
        trips.scheduled_start_local,
        trips.scheduled_end_local
)


-- ============================================================
-- Update only affected trips
-- ============================================================

UPDATE trip_operations AS trips

SET

    scheduled_duration_seconds =
        coverage.scheduled_duration_seconds,

    collector_coverage_seconds =
        coverage.collector_coverage_seconds,

    collector_coverage_percent =

        CASE

            WHEN coverage.scheduled_duration_seconds > 0

            THEN ROUND(
                (
                    100.0
                    * coverage.collector_coverage_seconds
                    / coverage.scheduled_duration_seconds
                )::numeric,
                2
            )

            ELSE 0

        END,

    updated_at = NOW()

FROM trip_coverage AS coverage

WHERE
    trips.feed_checksum =
        coverage.feed_checksum

    AND trips.service_date =
        coverage.service_date

    AND trips.trip_id =
        coverage.trip_id;


-- ============================================================
-- Advance watermark
-- ============================================================

UPDATE pipeline_watermarks

SET
    last_processed_at = NOW(),
    updated_at = NOW()

WHERE transformation_name =
    'update_trip_operations_coverage';