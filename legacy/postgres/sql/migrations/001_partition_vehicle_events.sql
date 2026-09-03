-- ============================================================
-- Migration 001: Partition realtime vehicle events
--
-- Purpose
-- -------
-- Migrate an existing installation from the original
-- unpartitioned vehicle_events table to:
--
--     vehicle_event_registry
--             +
--     vehicle_events_v2
--         PARTITION BY RANGE (ingestion_timestamp)
--
-- vehicle_event_registry preserves global event_key uniqueness.
--
-- vehicle_events_v2 stores the full realtime observations in
-- daily UTC partitions.
--
-- vehicle_event_schedule_matches is migrated from the old:
--
--     event_key -> vehicle_events(event_key)
--
-- relationship to the partition-aware:
--
--     (event_key, event_ingestion_timestamp)
--         -> vehicle_events_v2(
--                event_key,
--                ingestion_timestamp
--            )
--
-- IMPORTANT
-- ---------
-- This migration intentionally leaves the legacy
-- vehicle_events table in place after cutover. It is no longer
-- part of the active ingestion path, but keeping it temporarily
-- provides a rollback/reference copy until the migration has
-- been fully validated.
--
-- Large backfills are written to be resumable where practical.
-- ============================================================


-- ============================================================
-- 1. Global event-key registry
--
-- PostgreSQL requires a UNIQUE / PRIMARY KEY constraint on a
-- partitioned table to include its partition key.
--
-- event_key is logically unique across all ingestion dates, so
-- a narrow non-partitioned registry enforces that global rule.
-- ============================================================

CREATE TABLE IF NOT EXISTS vehicle_event_registry (

    event_key TEXT PRIMARY KEY,

    ingestion_timestamp TIMESTAMPTZ NOT NULL
);


-- Backfill the registry from the legacy event table.
--
-- ON CONFLICT makes this step safe to resume.

INSERT INTO vehicle_event_registry (
    event_key,
    ingestion_timestamp
)

SELECT
    event_key,
    ingestion_timestamp

FROM vehicle_events

WHERE
    ingestion_timestamp IS NOT NULL

ON CONFLICT (event_key)
DO NOTHING;


-- Fail clearly if the legacy table contains events that cannot
-- be represented in the partitioned design.

DO $$

BEGIN

    IF EXISTS (

        SELECT 1

        FROM vehicle_events

        WHERE ingestion_timestamp IS NULL

    ) THEN

        RAISE EXCEPTION
            'vehicle_events contains NULL ingestion_timestamp values';

    END IF;

END

$$;


-- ============================================================
-- 2. Partitioned realtime event parent
-- ============================================================

CREATE TABLE IF NOT EXISTS vehicle_events_v2 (

    event_key TEXT NOT NULL,

    entity_id TEXT,

    vehicle_id TEXT,

    trip_id TEXT,

    route_id TEXT,

    schedule_relationship TEXT,

    direction_id INTEGER,


    latitude DOUBLE PRECISION,

    longitude DOUBLE PRECISION,


    stop_id TEXT,

    current_stop_sequence INTEGER,

    current_status INTEGER,


    vehicle_timestamp TIMESTAMPTZ,

    feed_timestamp TIMESTAMPTZ,

    ingestion_timestamp TIMESTAMPTZ NOT NULL,


    created_at TIMESTAMPTZ DEFAULT NOW(),


    CONSTRAINT vehicle_events_v2_pkey

        PRIMARY KEY (
            event_key,
            ingestion_timestamp
        ),


    CHECK (
        latitude IS NULL
        OR latitude BETWEEN -90 AND 90
    ),


    CHECK (
        longitude IS NULL
        OR longitude BETWEEN -180 AND 180
    )
)

PARTITION BY RANGE (
    ingestion_timestamp
);


-- ============================================================
-- 3. Create daily UTC partitions for existing legacy history
--
-- Future partitions are created automatically by
-- src/loading/incremental_vehicle_loader.py before new batches
-- are inserted.
-- ============================================================

DO $$

DECLARE

    first_date DATE;

    last_date DATE;

    partition_date DATE;

    partition_name TEXT;

    partition_start TEXT;

    partition_end TEXT;

BEGIN

    SELECT
        MIN(
            ingestion_timestamp
            AT TIME ZONE 'UTC'
        )::DATE,

        MAX(
            ingestion_timestamp
            AT TIME ZONE 'UTC'
        )::DATE

    INTO
        first_date,
        last_date

    FROM vehicle_events;


    IF first_date IS NULL THEN

        RETURN;

    END IF;


    partition_date := first_date;


    WHILE partition_date <= last_date LOOP

        partition_name := format(
            'vehicle_events_v2_%s',
            to_char(
                partition_date,
                'YYYY_MM_DD'
            )
        );


        partition_start := (
            to_char(
                partition_date,
                'YYYY-MM-DD'
            )
            || ' 00:00:00+00'
        );


        partition_end := (
            to_char(
                partition_date + 1,
                'YYYY-MM-DD'
            )
            || ' 00:00:00+00'
        );


        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I '
            'PARTITION OF vehicle_events_v2 '
            'FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            partition_start,
            partition_end
        );


        partition_date :=
            partition_date + 1;

    END LOOP;

END

$$;


-- ============================================================
-- 4. Backfill full realtime observations
--
-- The composite primary key makes this resumable.
-- ============================================================

INSERT INTO vehicle_events_v2 (

    event_key,

    entity_id,

    vehicle_id,

    trip_id,

    route_id,

    schedule_relationship,

    direction_id,


    latitude,

    longitude,


    stop_id,

    current_stop_sequence,

    current_status,


    vehicle_timestamp,

    feed_timestamp,

    ingestion_timestamp,

    created_at
)

SELECT

    event_key,

    entity_id,

    vehicle_id,

    trip_id,

    route_id,

    schedule_relationship,

    direction_id,


    latitude,

    longitude,


    stop_id,

    current_stop_sequence,

    current_status,


    vehicle_timestamp,

    feed_timestamp,

    ingestion_timestamp,

    created_at

FROM vehicle_events

ON CONFLICT (
    event_key,
    ingestion_timestamp
)

DO NOTHING;


ANALYZE
    vehicle_event_registry;


ANALYZE
    vehicle_events_v2;


-- ============================================================
-- 5. Partitioned vehicle-event indexes
--
-- Creating these on the partitioned parent creates matching
-- indexes on the child partitions.
-- ============================================================

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_v2_ingestion_timestamp

ON vehicle_events_v2 (
    ingestion_timestamp
);


CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_v2_scheduled_ingestion_timestamp

ON vehicle_events_v2 (
    ingestion_timestamp
)

WHERE
    schedule_relationship = 'SCHEDULED';


CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_v2_scheduled_trip_timestamp

ON vehicle_events_v2 (
    trip_id,
    vehicle_timestamp
)

WHERE
    schedule_relationship = 'SCHEDULED';


CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_v2_trip_vehicle_timestamp

ON vehicle_events_v2 (
    trip_id,
    vehicle_id,
    vehicle_timestamp
);


-- ============================================================
-- 6. Add the partition key to schedule-match rows
-- ============================================================

ALTER TABLE vehicle_event_schedule_matches

ADD COLUMN IF NOT EXISTS
    event_ingestion_timestamp TIMESTAMPTZ;


-- Backfill only rows that still need the partition key.
--
-- This can be expensive on a large existing table because
-- PostgreSQL must create new row versions. It is intentionally
-- constrained to NULL rows so a partial run can be resumed.

UPDATE vehicle_event_schedule_matches
    AS matches

SET
    event_ingestion_timestamp =
        registry.ingestion_timestamp

FROM vehicle_event_registry
    AS registry

WHERE
    matches.event_key =
        registry.event_key

    AND matches.event_ingestion_timestamp
        IS NULL;


-- Do not continue if any schedule match could not be linked to
-- its event registry row.

DO $$

BEGIN

    IF EXISTS (

        SELECT 1

        FROM vehicle_event_schedule_matches

        WHERE event_ingestion_timestamp
            IS NULL

    ) THEN

        RAISE EXCEPTION
            'Some schedule matches are missing event_ingestion_timestamp';

    END IF;

END

$$;


ALTER TABLE vehicle_event_schedule_matches

ALTER COLUMN event_ingestion_timestamp
SET NOT NULL;


-- ============================================================
-- 7. Schedule-match indexes used by the incremental pipeline
-- ============================================================

CREATE INDEX IF NOT EXISTS
    idx_vehicle_event_schedule_matches_event_partition

ON vehicle_event_schedule_matches (
    event_key,
    event_ingestion_timestamp
);


CREATE INDEX IF NOT EXISTS
    idx_vehicle_schedule_matches_created_at

ON vehicle_event_schedule_matches (
    created_at
);


CREATE INDEX IF NOT EXISTS
    idx_vehicle_schedule_matches_created_trip

ON vehicle_event_schedule_matches (
    created_at
)

INCLUDE (
    feed_checksum,
    service_date,
    trip_id
);


CREATE INDEX IF NOT EXISTS
    idx_vehicle_schedule_matches_trip_stop

ON vehicle_event_schedule_matches (
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence
);


ANALYZE
    vehicle_event_schedule_matches;


-- ============================================================
-- 8. Add and validate the partition-aware foreign key
--
-- NOT VALID avoids performing the full validation while the
-- constraint is first added. VALIDATE then performs the check
-- explicitly before the legacy FK is removed.
-- ============================================================

DO $$

BEGIN

    IF NOT EXISTS (

        SELECT 1

        FROM pg_constraint

        WHERE
            conrelid =
                'vehicle_event_schedule_matches'::regclass

            AND conname =
                'vehicle_event_schedule_matches_v2_fkey'

    ) THEN

        ALTER TABLE
            vehicle_event_schedule_matches

        ADD CONSTRAINT
            vehicle_event_schedule_matches_v2_fkey

        FOREIGN KEY (
            event_key,
            event_ingestion_timestamp
        )

        REFERENCES vehicle_events_v2 (
            event_key,
            ingestion_timestamp
        )

        NOT VALID;

    END IF;

END

$$;


ALTER TABLE
    vehicle_event_schedule_matches

VALIDATE CONSTRAINT
    vehicle_event_schedule_matches_v2_fkey;


-- ============================================================
-- 9. Remove the obsolete foreign key to legacy vehicle_events
--
-- This is done only after the new partition-aware FK has been
-- validated successfully.
-- ============================================================

ALTER TABLE vehicle_event_schedule_matches

DROP CONSTRAINT IF EXISTS
    vehicle_event_schedule_matches_event_key_fkey;


-- ============================================================
-- 10. Final validation
-- ============================================================

DO $$

DECLARE

    registry_count BIGINT;

    partitioned_count BIGINT;

    missing_partitioned_events BIGINT;

    missing_schedule_match_events BIGINT;

BEGIN

    SELECT COUNT(*)

    INTO registry_count

    FROM vehicle_event_registry;


    SELECT COUNT(*)

    INTO partitioned_count

    FROM vehicle_events_v2;


    SELECT COUNT(*)

    INTO missing_partitioned_events

    FROM vehicle_event_registry
        AS registry

    LEFT JOIN vehicle_events_v2
        AS events

      ON events.event_key =
            registry.event_key

     AND events.ingestion_timestamp =
            registry.ingestion_timestamp

    WHERE
        events.event_key IS NULL;


    SELECT COUNT(*)

    INTO missing_schedule_match_events

    FROM vehicle_event_schedule_matches
        AS matches

    LEFT JOIN vehicle_events_v2
        AS events

      ON events.event_key =
            matches.event_key

     AND events.ingestion_timestamp =
            matches.event_ingestion_timestamp

    WHERE
        events.event_key IS NULL;


    IF registry_count <> partitioned_count THEN

        RAISE EXCEPTION
            'Registry count (%) does not equal partitioned event count (%)',
            registry_count,
            partitioned_count;

    END IF;


    IF missing_partitioned_events <> 0 THEN

        RAISE EXCEPTION
            '% registry events are missing from vehicle_events_v2',
            missing_partitioned_events;

    END IF;


    IF missing_schedule_match_events <> 0 THEN

        RAISE EXCEPTION
            '% schedule matches are missing their vehicle_events_v2 row',
            missing_schedule_match_events;

    END IF;

END

$$;


-- ============================================================
-- Migration complete
--
-- Active realtime storage:
--
--     vehicle_event_registry
--     vehicle_events_v2
--
-- Legacy vehicle_events remains present as a historical
-- migration/rollback copy, but the active loader and downstream
-- transformations should no longer read from or write to it.
-- ============================================================
