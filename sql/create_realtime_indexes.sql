-- ============================================================
-- Realtime pipeline performance indexes
-- ============================================================


-- ------------------------------------------------------------
-- 1. Schedule matching
--
-- vehicle_events is frequently accessed by trip and timestamp.
-- Restrict this index to scheduled realtime trips because
-- ADDED trips are not matched against static GTFS.
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_scheduled_trip_timestamp
ON vehicle_events (
    trip_id,
    vehicle_timestamp
)
WHERE schedule_relationship = 'SCHEDULED';


-- ------------------------------------------------------------
-- 2. Stop-event inference
--
-- We order observations within a vehicle/trip over time when
-- reconstructing stop transitions.
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_vehicle_events_trip_vehicle_timestamp
ON vehicle_events (
    trip_id,
    vehicle_id,
    vehicle_timestamp
);


-- ------------------------------------------------------------
-- 3. GTFS schedule matching
--
-- Existing PK is:
--
-- feed_checksum, service_date, trip_id
--
-- But realtime matching commonly starts from trip_id.
-- PostgreSQL cannot efficiently use the existing PK for that
-- access pattern because trip_id is the third column.
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_gtfs_trip_instances_trip_date
ON gtfs_trip_instances (
    trip_id,
    service_date,
    feed_checksum
);


-- ------------------------------------------------------------
-- 4. Realtime matched events
--
-- Existing PK is only event_key.
--
-- Downstream transformations group and join by:
-- feed + date + trip + stop sequence.
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
-- 5. Collector coverage calculations
-- ------------------------------------------------------------

CREATE INDEX IF NOT EXISTS
    idx_processed_files_ingestion_timestamp
ON processed_files (
    ingestion_timestamp
);

CREATE INDEX IF NOT EXISTS
    idx_vehicle_schedule_matches_created_at
ON vehicle_event_schedule_matches (
    created_at
);

CREATE INDEX IF NOT EXISTS
    idx_processed_files_processed_at
ON processed_files (
    processed_at
);


CREATE INDEX IF NOT EXISTS
    idx_trip_operations_service_date_schedule
ON trip_operations (
    service_date,
    scheduled_start_local,
    scheduled_end_local
);

CREATE INDEX IF NOT EXISTS
    idx_processed_files_file_path
ON processed_files (
    file_path
);