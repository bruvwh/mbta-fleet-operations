-- ============================================================
-- MBTA Fleet Operations Data Platform
-- PostgreSQL Schema
-- ============================================================


-- ============================================================
-- REALTIME VEHICLE DATA
--
-- Realtime observations are stored in a range-partitioned
-- parent table by ingestion_timestamp.
--
-- event_key uniqueness across all partitions is enforced by
-- vehicle_event_registry before rows are inserted.
--
-- Daily child partitions are created automatically by the
-- incremental loader as new ingestion dates arrive.
-- ============================================================


-- ------------------------------------------------------------
-- Global event-key registry
--
-- PostgreSQL requires a unique/primary-key constraint on a
-- partitioned table to include the partition key. Because the
-- logical event_key must remain globally unique across dates,
-- this narrow registry provides that global uniqueness check.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS vehicle_event_registry (
    event_key TEXT PRIMARY KEY,
    ingestion_timestamp TIMESTAMPTZ NOT NULL
);


-- ------------------------------------------------------------
-- Partitioned realtime vehicle observations
-- ------------------------------------------------------------

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


-- Tracks which raw realtime protobuf files have already been loaded
CREATE TABLE IF NOT EXISTS processed_files (
    file_checksum TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,

    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW(),

    event_count INTEGER NOT NULL
);


-- Tracks incremental transformation watermarks.
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    transformation_name TEXT PRIMARY KEY,

    last_processed_at TIMESTAMPTZ NOT NULL,

    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- STATIC GTFS FEED VERSIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS gtfs_feeds (
    feed_checksum TEXT PRIMARY KEY,

    feed_version TEXT,
    feed_start_date DATE,
    feed_end_date DATE,

    file_path TEXT NOT NULL,

    downloaded_at TIMESTAMPTZ DEFAULT NOW(),
    schedule_loaded_at TIMESTAMPTZ
);


-- ============================================================
-- STATIC GTFS ROUTES
-- ============================================================

CREATE TABLE IF NOT EXISTS gtfs_routes (
    feed_checksum TEXT NOT NULL,
    route_id TEXT NOT NULL,

    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    route_color TEXT,
    route_text_color TEXT,

    PRIMARY KEY (
        feed_checksum,
        route_id
    ),

    FOREIGN KEY (feed_checksum)
        REFERENCES gtfs_feeds(feed_checksum)
);


-- ============================================================
-- STATIC GTFS STOPS
-- ============================================================

CREATE TABLE IF NOT EXISTS gtfs_stops (
    feed_checksum TEXT NOT NULL,
    stop_id TEXT NOT NULL,

    stop_name TEXT,

    stop_lat DOUBLE PRECISION,
    stop_lon DOUBLE PRECISION,

    location_type INTEGER,
    parent_station TEXT,
    platform_code TEXT,
    wheelchair_boarding INTEGER,

    PRIMARY KEY (
        feed_checksum,
        stop_id
    ),

    FOREIGN KEY (feed_checksum)
        REFERENCES gtfs_feeds(feed_checksum),

    CHECK (
        stop_lat IS NULL
        OR stop_lat BETWEEN -90 AND 90
    ),

    CHECK (
        stop_lon IS NULL
        OR stop_lon BETWEEN -180 AND 180
    )
);


-- ============================================================
-- STATIC GTFS TRIPS
-- ============================================================

CREATE TABLE IF NOT EXISTS gtfs_trips (
    feed_checksum TEXT NOT NULL,
    trip_id TEXT NOT NULL,

    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,

    trip_headsign TEXT,
    direction_id INTEGER,
    block_id TEXT,
    shape_id TEXT,
    wheelchair_accessible INTEGER,

    PRIMARY KEY (
        feed_checksum,
        trip_id
    ),

    FOREIGN KEY (
        feed_checksum,
        route_id
    )
        REFERENCES gtfs_routes(
            feed_checksum,
            route_id
        )
);


-- ============================================================
-- STATIC GTFS STOP TIMES
-- ============================================================

CREATE TABLE IF NOT EXISTS gtfs_stop_times (
    feed_checksum TEXT NOT NULL,
    trip_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,

    stop_id TEXT NOT NULL,

    arrival_time TEXT,
    departure_time TEXT,

    arrival_seconds INTEGER,
    departure_seconds INTEGER,

    pickup_type INTEGER,
    drop_off_type INTEGER,
    timepoint INTEGER,

    PRIMARY KEY (
        feed_checksum,
        trip_id,
        stop_sequence
    ),

    FOREIGN KEY (
        feed_checksum,
        trip_id
    )
        REFERENCES gtfs_trips(
            feed_checksum,
            trip_id
        ),

    FOREIGN KEY (
        feed_checksum,
        stop_id
    )
        REFERENCES gtfs_stops(
            feed_checksum,
            stop_id
        )
);


-- ============================================================
-- GTFS SERVICE CALENDAR
-- ============================================================

CREATE TABLE IF NOT EXISTS gtfs_calendar (
    feed_checksum TEXT NOT NULL,
    service_id TEXT NOT NULL,

    monday INTEGER NOT NULL,
    tuesday INTEGER NOT NULL,
    wednesday INTEGER NOT NULL,
    thursday INTEGER NOT NULL,
    friday INTEGER NOT NULL,
    saturday INTEGER NOT NULL,
    sunday INTEGER NOT NULL,

    start_date DATE NOT NULL,
    end_date DATE NOT NULL,

    PRIMARY KEY (
        feed_checksum,
        service_id
    ),

    FOREIGN KEY (feed_checksum)
        REFERENCES gtfs_feeds(feed_checksum)
);


-- ============================================================
-- GTFS SERVICE CALENDAR EXCEPTIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS gtfs_calendar_dates (
    feed_checksum TEXT NOT NULL,
    service_id TEXT NOT NULL,
    service_date DATE NOT NULL,

    exception_type INTEGER NOT NULL,

    PRIMARY KEY (
        feed_checksum,
        service_id,
        service_date
    ),

    FOREIGN KEY (feed_checksum)
        REFERENCES gtfs_feeds(feed_checksum),

    CHECK (
        exception_type IN (1, 2)
    )
);


-- ============================================================
-- FUNCTION: DETERMINE ACTIVE SERVICES FOR A DATE
-- ============================================================

CREATE OR REPLACE FUNCTION active_gtfs_services(
    p_feed_checksum TEXT,
    p_service_date DATE
)
RETURNS TABLE(service_id TEXT)

LANGUAGE SQL

AS $$

    WITH base_services AS (

        SELECT
            calendar.service_id

        FROM gtfs_calendar AS calendar

        WHERE calendar.feed_checksum = p_feed_checksum

          AND p_service_date
              BETWEEN calendar.start_date
              AND calendar.end_date

          AND CASE EXTRACT(
                ISODOW FROM p_service_date
              )

                WHEN 1 THEN calendar.monday
                WHEN 2 THEN calendar.tuesday
                WHEN 3 THEN calendar.wednesday
                WHEN 4 THEN calendar.thursday
                WHEN 5 THEN calendar.friday
                WHEN 6 THEN calendar.saturday
                WHEN 7 THEN calendar.sunday

              END = 1
    ),


    added_services AS (

        SELECT
            calendar_dates.service_id

        FROM gtfs_calendar_dates AS calendar_dates

        WHERE calendar_dates.feed_checksum = p_feed_checksum
          AND calendar_dates.service_date = p_service_date
          AND calendar_dates.exception_type = 1
    ),


    removed_services AS (

        SELECT
            calendar_dates.service_id

        FROM gtfs_calendar_dates AS calendar_dates

        WHERE calendar_dates.feed_checksum = p_feed_checksum
          AND calendar_dates.service_date = p_service_date
          AND calendar_dates.exception_type = 2
    ),


    active_services AS (

        SELECT service_id
        FROM base_services

        UNION

        SELECT service_id
        FROM added_services
    )


    SELECT
        active.service_id

    FROM active_services AS active

    WHERE NOT EXISTS (

        SELECT 1

        FROM removed_services AS removed

        WHERE removed.service_id = active.service_id
    );

$$;


-- ============================================================
-- MATERIALIZED ACTIVE SERVICES BY DATE
-- ============================================================

CREATE TABLE IF NOT EXISTS gtfs_active_services (
    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    service_id TEXT NOT NULL,

    PRIMARY KEY (
        feed_checksum,
        service_date,
        service_id
    ),

    FOREIGN KEY (feed_checksum)
        REFERENCES gtfs_feeds(feed_checksum)
);


-- ============================================================
-- SCHEDULED TRIP INSTANCES
--
-- One row represents:
-- "This GTFS trip is scheduled to operate on this service date."
-- ============================================================

CREATE TABLE IF NOT EXISTS gtfs_trip_instances (
    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,

    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    direction_id INTEGER,

    PRIMARY KEY (
        feed_checksum,
        service_date,
        trip_id
    ),

    FOREIGN KEY (
        feed_checksum,
        trip_id
    )
        REFERENCES gtfs_trips(
            feed_checksum,
            trip_id
        )
);


-- ============================================================
-- REALTIME EVENT → SCHEDULE MATCH
--
-- Connects a realtime vehicle observation to the corresponding
-- scheduled GTFS trip instance and stop.
--
-- event_ingestion_timestamp is stored with event_key so the
-- match can reference the exact row in the partitioned
-- vehicle_events_v2 table.
-- ============================================================

CREATE TABLE IF NOT EXISTS vehicle_event_schedule_matches (
    event_key TEXT PRIMARY KEY,

    event_ingestion_timestamp TIMESTAMPTZ NOT NULL,

    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,

    stop_sequence INTEGER,

    scheduled_local_time TIMESTAMP,
    difference_seconds DOUBLE PRECISION,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    FOREIGN KEY (
        event_key,
        event_ingestion_timestamp
    )
        REFERENCES vehicle_events_v2(
            event_key,
            ingestion_timestamp
        ),

    FOREIGN KEY (
        feed_checksum,
        service_date,
        trip_id
    )
        REFERENCES gtfs_trip_instances(
            feed_checksum,
            service_date,
            trip_id
        ),

    CHECK (
        difference_seconds IS NULL
        OR difference_seconds >= 0
    )
);

CREATE TABLE IF NOT EXISTS trip_realtime_summary (
    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,

    route_id TEXT NOT NULL,
    direction_id INTEGER,

    first_observation TIMESTAMPTZ,
    last_observation TIMESTAMPTZ,

    observation_count INTEGER NOT NULL,
    distinct_vehicle_count INTEGER NOT NULL,

    first_stop_sequence INTEGER,
    last_stop_sequence INTEGER,

    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        feed_checksum,
        service_date,
        trip_id
    ),

    FOREIGN KEY (
        feed_checksum,
        service_date,
        trip_id
    )
        REFERENCES gtfs_trip_instances(
            feed_checksum,
            service_date,
            trip_id
        )
);

CREATE TABLE IF NOT EXISTS gtfs_trip_schedule_summary (
    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,

    scheduled_start_local TIMESTAMP,
    scheduled_end_local TIMESTAMP,

    scheduled_stop_count INTEGER NOT NULL,

    PRIMARY KEY (
        feed_checksum,
        service_date,
        trip_id
    ),

    FOREIGN KEY (
        feed_checksum,
        service_date,
        trip_id
    )
        REFERENCES gtfs_trip_instances(
            feed_checksum,
            service_date,
            trip_id
        )
);

CREATE TABLE IF NOT EXISTS trip_operations (
    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,

    route_id TEXT NOT NULL,
    direction_id INTEGER,

    scheduled_start_local TIMESTAMP,
    scheduled_end_local TIMESTAMP,
    scheduled_stop_count INTEGER,

    scheduled_duration_seconds DOUBLE PRECISION,
    collector_coverage_seconds DOUBLE PRECISION,
    collector_coverage_percent DOUBLE PRECISION,

    realtime_observed BOOLEAN NOT NULL,

    first_observation TIMESTAMPTZ,
    last_observation TIMESTAMPTZ,

    observation_count INTEGER,
    distinct_vehicle_count INTEGER,

    first_stop_sequence INTEGER,
    last_stop_sequence INTEGER,

    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        feed_checksum,
        service_date,
        trip_id
    ),

    FOREIGN KEY (
        feed_checksum,
        service_date,
        trip_id
    )
        REFERENCES gtfs_trip_instances(
            feed_checksum,
            service_date,
            trip_id
        )
);

CREATE TABLE IF NOT EXISTS collector_coverage_windows (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,

    snapshot_count INTEGER NOT NULL,

    PRIMARY KEY (
        window_start,
        window_end
    ),

    CHECK (window_end >= window_start),
    CHECK (snapshot_count > 0)
);

CREATE TABLE IF NOT EXISTS historical_lamp_files (
    file_checksum TEXT PRIMARY KEY,

    service_date DATE NOT NULL,
    file_path TEXT NOT NULL,

    downloaded_at TIMESTAMPTZ DEFAULT NOW(),
    loaded_at TIMESTAMPTZ,

    row_count INTEGER
);

CREATE TABLE IF NOT EXISTS historical_stop_events (
    file_checksum TEXT NOT NULL,

    service_date DATE NOT NULL,

    route_id TEXT,
    branch_route_id TEXT,
    trunk_route_id TEXT,

    trip_id TEXT NOT NULL,

    stop_id TEXT NOT NULL,
    parent_station TEXT,
    stop_sequence INTEGER NOT NULL,

    direction_id INTEGER,
    direction TEXT,
    direction_destination TEXT,

    vehicle_id TEXT,
    vehicle_label TEXT,
    vehicle_consist TEXT,

    stop_count INTEGER,

    start_seconds INTEGER,

    move_timestamp TIMESTAMPTZ,
    stop_timestamp TIMESTAMPTZ,

    travel_time_seconds BIGINT,
    dwell_time_seconds BIGINT,

    headway_branch_seconds BIGINT,
    headway_trunk_seconds BIGINT,

    scheduled_arrival_seconds BIGINT,
    scheduled_departure_seconds BIGINT,

    scheduled_travel_time_seconds BIGINT,

    scheduled_headway_branch_seconds BIGINT,
    scheduled_headway_trunk_seconds BIGINT,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        service_date,
        trip_id,
        stop_sequence,
        stop_id
    ),

    FOREIGN KEY (file_checksum)
        REFERENCES historical_lamp_files(file_checksum),

    CHECK (
        direction_id IS NULL
        OR direction_id IN (0, 1)
    )
);

CREATE TABLE IF NOT EXISTS realtime_stop_events (
    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    stop_id TEXT NOT NULL,

    route_id TEXT,
    direction_id INTEGER,

    vehicle_id TEXT,
    vehicle_count INTEGER NOT NULL,

    scheduled_local_time TIMESTAMP,

    arrival_lower_bound TIMESTAMPTZ,
    arrival_upper_bound TIMESTAMPTZ,
    arrival_estimate TIMESTAMPTZ,

    departure_lower_bound TIMESTAMPTZ,
    departure_upper_bound TIMESTAMPTZ,
    departure_estimate TIMESTAMPTZ,

    arrival_uncertainty_seconds DOUBLE PRECISION,
    departure_uncertainty_seconds DOUBLE PRECISION,

    stopped_observation_count INTEGER NOT NULL,
    total_observation_count INTEGER NOT NULL,

    inference_quality TEXT NOT NULL,

    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        feed_checksum,
        service_date,
        trip_id,
        stop_sequence,
        stop_id
    ),

    FOREIGN KEY (
        feed_checksum,
        service_date,
        trip_id
    )
    REFERENCES gtfs_trip_instances (
        feed_checksum,
        service_date,
        trip_id
    )
);

CREATE TABLE IF NOT EXISTS gtfs_stop_instances (
    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    stop_id TEXT NOT NULL,

    route_id TEXT,
    direction_id INTEGER,

    scheduled_arrival_local TIMESTAMP,
    scheduled_departure_local TIMESTAMP,

    arrival_seconds INTEGER,
    departure_seconds INTEGER,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        feed_checksum,
        service_date,
        trip_id,
        stop_sequence,
        stop_id
    ),

    FOREIGN KEY (
        feed_checksum,
        service_date,
        trip_id
    )
    REFERENCES gtfs_trip_instances (
        feed_checksum,
        service_date,
        trip_id
    )
);

CREATE TABLE IF NOT EXISTS realtime_stop_performance (
    feed_checksum TEXT NOT NULL,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    stop_id TEXT NOT NULL,

    route_id TEXT,
    direction_id INTEGER,
    vehicle_id TEXT,
    arrival_bound_method TEXT,

    scheduled_arrival TIMESTAMPTZ,
    scheduled_departure TIMESTAMPTZ,

    arrival_estimate TIMESTAMPTZ,
    departure_estimate TIMESTAMPTZ,

    arrival_lower_bound TIMESTAMPTZ,
    arrival_upper_bound TIMESTAMPTZ,

    departure_lower_bound TIMESTAMPTZ,
    departure_upper_bound TIMESTAMPTZ,

    arrival_deviation_seconds DOUBLE PRECISION,
    departure_deviation_seconds DOUBLE PRECISION,

    arrival_deviation_lower_seconds DOUBLE PRECISION,
    arrival_deviation_upper_seconds DOUBLE PRECISION,

    departure_deviation_lower_seconds DOUBLE PRECISION,
    departure_deviation_upper_seconds DOUBLE PRECISION,

    arrival_uncertainty_seconds DOUBLE PRECISION,
    departure_uncertainty_seconds DOUBLE PRECISION,

    inference_quality TEXT NOT NULL,

    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        feed_checksum,
        service_date,
        trip_id,
        stop_sequence,
        stop_id
    )
);

