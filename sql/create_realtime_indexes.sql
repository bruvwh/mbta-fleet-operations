-- ============================================================
-- Realtime pipeline performance indexes
--
-- During the vehicle_events partition migration, both the
-- legacy vehicle_events table and partitioned
-- vehicle_events_v2 table remain in use.
--
-- Legacy indexes should remain until all downstream
-- transformations have been migrated to vehicle_events_v2.
-- ============================================================


-- ============================================================
-- 1. LEGACY vehicle_events indexes
--
-- TEMPORARY during migration.
--
-- Some downstream transformations still read vehicle_events,
-- so these indexes must remain until cutover is complete.
-- ============================================================


-- ------------------------------------------------------------
-- 1a. Incremental ingestion / recent-event access
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_ingestion_timestamp
ON vehicle_events (
    ingestion_timestamp
);


-- ------------------------------------------------------------
-- 1b. Schedule matching
--
-- Only scheduled realtime trips can be matched against static
-- GTFS.
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_scheduled_ingestion_timestamp
ON vehicle_events (
    ingestion_timestamp
)
WHERE schedule_relationship = 'SCHEDULED';


CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_scheduled_trip_timestamp
ON vehicle_events (
    trip_id,
    vehicle_timestamp
)
WHERE schedule_relationship = 'SCHEDULED';


-- ------------------------------------------------------------
-- 1c. Stop-event inference
--
-- Observations are ordered by trip, vehicle, and event time
-- when reconstructing stop transitions.
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_trip_vehicle_timestamp
ON vehicle_events (
    trip_id,
    vehicle_id,
    vehicle_timestamp
);


-- ============================================================
-- 2. PARTITIONED vehicle_events_v2 indexes
--
-- PostgreSQL creates corresponding local indexes on each
-- partition when these indexes are created on the partitioned
-- parent table.
-- ============================================================


-- ------------------------------------------------------------
-- 2a. Partition / ingestion-time access
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_v2_ingestion_timestamp
ON vehicle_events_v2 (
    ingestion_timestamp
);


-- ------------------------------------------------------------
-- 2b. Scheduled realtime events
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_v2_scheduled_ingestion_timestamp
ON vehicle_events_v2 (
    ingestion_timestamp
)
WHERE schedule_relationship = 'SCHEDULED';


CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_v2_scheduled_trip_timestamp
ON vehicle_events_v2 (
    trip_id,
    vehicle_timestamp
)
WHERE schedule_relationship = 'SCHEDULED';


-- ------------------------------------------------------------
-- 2c. Vehicle/trip chronological access
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_v2_trip_vehicle_timestamp
ON vehicle_events_v2 (
    trip_id,
    vehicle_id,
    vehicle_timestamp
);


-- ============================================================
-- 3. GTFS schedule matching
--
-- gtfs_trip_instances has a primary key beginning with:
--
--     feed_checksum,
--     service_date,
--     trip_id
--
-- Realtime schedule matching commonly starts from trip_id,
-- so a separate access path is useful.
-- ============================================================

CREATE INDEX IF NOT EXISTS
    idx_gtfs_trip_instances_trip_date
ON gtfs_trip_instances (
    trip_id,
    service_date,
    feed_checksum
);


-- ============================================================
-- 4. Realtime schedule-match indexes
-- ============================================================


-- ------------------------------------------------------------
-- 4a. Complete-trip retrieval
--
-- Stop-event inference identifies affected trips and then
-- retrieves all schedule matches belonging to those trips.
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
-- 4b. Incremental watermark lookup
--
-- Used to identify schedule matches created after the previous
-- transformation watermark.
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_schedule_matches_created_at
ON vehicle_event_schedule_matches (
    created_at
);


-- ------------------------------------------------------------
-- 4c. Covering index for affected-trip detection
--
-- Stop-event inference filters by created_at and immediately
-- needs:
--
--     feed_checksum
--     service_date
--     trip_id
--
-- INCLUDE allows PostgreSQL to obtain those trip identifiers
-- directly from the index rather than repeatedly returning to
-- the main schedule-match table.
--
-- In the controlled replay benchmark, this reduced the exact
-- affected-trip lookup from roughly 46 seconds to well under
-- one second when combined with accurate temporary-table
-- statistics.
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
-- 4d. Partition-aware event lookup
--
-- vehicle_events_v2 is identified by:
--
--     event_key,
--     ingestion_timestamp
--
-- Schedule matches now retain both values.
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_event_schedule_matches_event_partition
ON vehicle_event_schedule_matches (
    event_key,
    event_ingestion_timestamp
);


-- ============================================================
-- 5. Collector / processed-file indexes
-- ============================================================


-- ------------------------------------------------------------
-- 5a. Source ingestion coverage
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_processed_files_ingestion_timestamp
ON processed_files (
    ingestion_timestamp
);


-- ------------------------------------------------------------
-- 5b. Incremental loader watermark / processing history
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_processed_files_processed_at
ON processed_files (
    processed_at
);


-- ------------------------------------------------------------
-- 5c. Processed-file path lookup
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_processed_files_file_path
ON processed_files (
    file_path
);


-- ============================================================
-- 6. Trip operations / coverage calculations
-- ============================================================

CREATE INDEX IF NOT EXISTS
    idx_trip_operations_service_date_schedule
ON trip_operations (
    service_date,
    scheduled_start_local,
    scheduled_end_local
);