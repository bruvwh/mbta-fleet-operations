-- ============================================================
-- Realtime pipeline performance indexes
--
-- Active realtime storage:
--
--     vehicle_event_registry
--     vehicle_events_v2
--
-- The legacy vehicle_events table is retained only as a
-- migration / rollback copy and is no longer part of the
-- active pipeline.
-- ============================================================


-- ============================================================
-- 1. Partitioned vehicle_events_v2 indexes
--
-- Creating indexes on the partitioned parent creates
-- corresponding indexes on each child partition.
-- ============================================================


-- ------------------------------------------------------------
-- 1a. Ingestion-time access
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_v2_ingestion_timestamp

ON vehicle_events_v2 (
    ingestion_timestamp
);


-- ------------------------------------------------------------
-- 1b. Scheduled realtime observations
--
-- Static GTFS matching applies only to scheduled realtime
-- trips.
-- ------------------------------------------------------------

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


-- ------------------------------------------------------------
-- 1c. Vehicle/trip chronological access
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_v2_trip_vehicle_timestamp

ON vehicle_events_v2 (
    trip_id,
    vehicle_id,
    vehicle_timestamp
);


-- ============================================================
-- 2. GTFS schedule matching
--
-- gtfs_trip_instances has a primary key beginning with:
--
--     feed_checksum,
--     service_date,
--     trip_id
--
-- Realtime matching commonly starts from trip_id.
-- ============================================================

CREATE INDEX IF NOT EXISTS
    idx_gtfs_trip_instances_trip_date

ON gtfs_trip_instances (
    trip_id,
    service_date,
    feed_checksum
);


-- ============================================================
-- 3. Realtime schedule-match indexes
-- ============================================================


-- ------------------------------------------------------------
-- 3a. Complete-trip retrieval
--
-- Incremental transformations identify affected trips and then
-- retrieve all schedule matches belonging to those trips.
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_schedule_matches_trip_stop

ON vehicle_event_schedule_matches (
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence
);


-- ------------------------------------------------------------
-- 3b. Incremental watermark access
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_schedule_matches_created_at

ON vehicle_event_schedule_matches (
    created_at
);


-- ------------------------------------------------------------
-- 3c. Covering index for affected-trip detection
--
-- Transformations filter on created_at and immediately need
-- feed_checksum, service_date, and trip_id.
-- ------------------------------------------------------------

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


-- ------------------------------------------------------------
-- 3d. Partition-aware event lookup
--
-- Schedule matches retain both values needed to reference the
-- exact vehicle_events_v2 row.
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_event_schedule_matches_event_partition

ON vehicle_event_schedule_matches (
    event_key,
    event_ingestion_timestamp
);


-- ============================================================
-- 4. Collector / processed-file indexes
-- ============================================================

CREATE INDEX IF NOT EXISTS
    idx_processed_files_ingestion_timestamp

ON processed_files (
    ingestion_timestamp
);


CREATE INDEX IF NOT EXISTS
    idx_processed_files_processed_at

ON processed_files (
    processed_at
);


CREATE INDEX IF NOT EXISTS
    idx_processed_files_file_path

ON processed_files (
    file_path
);


-- ============================================================
-- 5. Trip operations / coverage calculations
-- ============================================================

CREATE INDEX IF NOT EXISTS
    idx_trip_operations_service_date_schedule

ON trip_operations (
    service_date,
    scheduled_start_local,
    scheduled_end_local
);