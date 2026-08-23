CREATE TABLE IF NOT EXISTS vehicle_events (
    event_key TEXT PRIMARY KEY,

    entity_id TEXT,
    vehicle_id TEXT,
    trip_id TEXT,
    route_id TEXT,
    direction_id INTEGER,

    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,

    stop_id TEXT,
    current_stop_sequence INTEGER,
    current_status INTEGER,

    vehicle_timestamp TIMESTAMPTZ,
    feed_timestamp TIMESTAMPTZ,
    ingestion_timestamp TIMESTAMPTZ,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CHECK (latitude BETWEEN -90 AND 90),
    CHECK (longitude BETWEEN -180 AND 180)
);

CREATE TABLE IF NOT EXISTS processed_files (
    file_checksum TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    event_count INTEGER NOT NULL
);