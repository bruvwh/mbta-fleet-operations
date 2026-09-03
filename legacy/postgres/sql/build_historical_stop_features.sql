-- ============================================================
-- Build historical_stop_features
--
-- Grain:
--   one row per
--   service_date + trip_id + stop_sequence + stop_id
--
-- Source:
--   historical_stop_events (MBTA LAMP)
--
-- Important timestamp interpretation:
--
--   stop_timestamp
--       = observed arrival at the CURRENT stop
--
--   move_timestamp
--       = movement toward the CURRENT stop
--
-- Therefore:
--
--   departure from current stop
--       ≈ move_timestamp associated with the NEXT stop
--
-- We obtain that using LEAD(move_timestamp).
-- ============================================================


-- ------------------------------------------------------------
-- 1. Create feature table
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS historical_stop_features (

    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    stop_id TEXT NOT NULL,

    route_id TEXT,
    direction_id INTEGER,
    vehicle_id TEXT,

    trip_type TEXT,

    scheduled_arrival TIMESTAMPTZ,
    scheduled_departure TIMESTAMPTZ,

    observed_arrival TIMESTAMPTZ,
    observed_departure TIMESTAMPTZ,

    arrival_deviation_seconds DOUBLE PRECISION,
    departure_deviation_seconds DOUBLE PRECISION,

    travel_time_seconds BIGINT,
    dwell_time_seconds BIGINT,

    headway_branch_seconds BIGINT,
    headway_trunk_seconds BIGINT,

    service_hour INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,

    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        service_date,
        trip_id,
        stop_sequence,
        stop_id
    )
);


-- ------------------------------------------------------------
-- 2. Compatibility for table created using older version
--
-- CREATE TABLE IF NOT EXISTS does not add new columns to an
-- existing table, so make sure trip_type exists.
-- ------------------------------------------------------------

ALTER TABLE historical_stop_features
ADD COLUMN IF NOT EXISTS trip_type TEXT;


-- ------------------------------------------------------------
-- 3. Order historical stop events within each trip
--
-- next_move_timestamp represents movement toward the NEXT stop.
-- That gives us an estimate of departure from the CURRENT stop.
-- ------------------------------------------------------------

WITH ordered_history AS (

    SELECT
        h.*,

        LEAD(h.move_timestamp) OVER (
            PARTITION BY
                h.service_date,
                h.trip_id
            ORDER BY
                h.stop_sequence,
                h.stop_id
        ) AS next_move_timestamp

    FROM historical_stop_events h

),


-- ------------------------------------------------------------
-- 4. Build features
-- ------------------------------------------------------------

features AS (

    SELECT

        h.service_date,
        h.trip_id,
        h.stop_sequence,
        h.stop_id,

        h.route_id,
        h.direction_id,
        h.vehicle_id,


        -- ----------------------------------------------------
        -- Trip type
        -- ----------------------------------------------------

        CASE
            WHEN h.trip_id LIKE 'ADDED-%'
                THEN 'ADDED'

            WHEN h.trip_id LIKE 'NONREV%'
                THEN 'NONREV'

            ELSE 'PLANNED'
        END AS trip_type,


        -- ----------------------------------------------------
        -- Scheduled arrival
        --
        -- Do NOT use MOD here.
        --
        -- GTFS-style schedule seconds can exceed 86,400 for
        -- service continuing after midnight.
        -- ----------------------------------------------------

        CASE
            WHEN h.scheduled_arrival_seconds IS NOT NULL
            THEN (
                h.service_date::timestamp
                + h.scheduled_arrival_seconds
                    * INTERVAL '1 second'
            ) AT TIME ZONE 'America/New_York'
        END AS scheduled_arrival,


        -- ----------------------------------------------------
        -- Scheduled departure
        -- ----------------------------------------------------

        CASE
            WHEN h.scheduled_departure_seconds IS NOT NULL
            THEN (
                h.service_date::timestamp
                + h.scheduled_departure_seconds
                    * INTERVAL '1 second'
            ) AT TIME ZONE 'America/New_York'
        END AS scheduled_departure,


        -- ----------------------------------------------------
        -- Observed arrival
        --
        -- LAMP stop_timestamp corresponds to observation
        -- at the current stop.
        -- ----------------------------------------------------

        h.stop_timestamp
            AS observed_arrival,


        -- ----------------------------------------------------
        -- Observed departure
        --
        -- Current row's move_timestamp is movement toward the
        -- current stop, so use the NEXT stop's move_timestamp.
        --
        -- Last stop of a trip naturally receives NULL.
        -- ----------------------------------------------------

        h.next_move_timestamp
            AS observed_departure,


        -- ----------------------------------------------------
        -- Arrival schedule deviation
        --
        -- positive = later than scheduled
        -- negative = earlier than scheduled
        -- ----------------------------------------------------

        CASE
            WHEN
                h.stop_timestamp IS NOT NULL
                AND h.scheduled_arrival_seconds IS NOT NULL

            THEN EXTRACT(
                EPOCH FROM (

                    h.stop_timestamp

                    -

                    (
                        h.service_date::timestamp
                        + h.scheduled_arrival_seconds
                            * INTERVAL '1 second'
                    ) AT TIME ZONE 'America/New_York'

                )
            )
        END AS arrival_deviation_seconds,


        -- ----------------------------------------------------
        -- Departure schedule deviation
        --
        -- Uses NEXT stop's move_timestamp as our estimate
        -- of departure from the current stop.
        -- ----------------------------------------------------

        CASE
            WHEN
                h.next_move_timestamp IS NOT NULL
                AND h.scheduled_departure_seconds IS NOT NULL

            THEN EXTRACT(
                EPOCH FROM (

                    h.next_move_timestamp

                    -

                    (
                        h.service_date::timestamp
                        + h.scheduled_departure_seconds
                            * INTERVAL '1 second'
                    ) AT TIME ZONE 'America/New_York'

                )
            )
        END AS departure_deviation_seconds,


        -- ----------------------------------------------------
        -- Operational metrics already supplied by LAMP
        -- ----------------------------------------------------

        h.travel_time_seconds,
        h.dwell_time_seconds,

        h.headway_branch_seconds,
        h.headway_trunk_seconds,


        -- ----------------------------------------------------
        -- Service hour
        --
        -- Here MOD is intentional.
        --
        -- Example:
        -- scheduled_arrival_seconds = 90,000
        -- GTFS equivalent = 25:00
        --
        -- actual scheduled timestamp remains next calendar day,
        -- but clock-hour feature becomes 1.
        -- ----------------------------------------------------

        CASE
            WHEN h.scheduled_arrival_seconds IS NOT NULL
            THEN FLOOR(
                MOD(
                    h.scheduled_arrival_seconds,
                    86400
                ) / 3600.0
            )::INTEGER
        END AS service_hour,


        -- ISO weekday:
        -- Monday = 1
        -- ...
        -- Sunday = 7

        EXTRACT(
            ISODOW FROM h.service_date
        )::INTEGER AS day_of_week,


        -- Weekend flag

        (
            EXTRACT(ISODOW FROM h.service_date)
            IN (6, 7)
        ) AS is_weekend

    FROM ordered_history h
)


-- ------------------------------------------------------------
-- 5. Insert/upsert into feature table
-- ------------------------------------------------------------

INSERT INTO historical_stop_features (

    service_date,
    trip_id,
    stop_sequence,
    stop_id,

    route_id,
    direction_id,
    vehicle_id,

    trip_type,

    scheduled_arrival,
    scheduled_departure,

    observed_arrival,
    observed_departure,

    arrival_deviation_seconds,
    departure_deviation_seconds,

    travel_time_seconds,
    dwell_time_seconds,

    headway_branch_seconds,
    headway_trunk_seconds,

    service_hour,
    day_of_week,
    is_weekend,

    updated_at
)

SELECT

    service_date,
    trip_id,
    stop_sequence,
    stop_id,

    route_id,
    direction_id,
    vehicle_id,

    trip_type,

    scheduled_arrival,
    scheduled_departure,

    observed_arrival,
    observed_departure,

    arrival_deviation_seconds,
    departure_deviation_seconds,

    travel_time_seconds,
    dwell_time_seconds,

    headway_branch_seconds,
    headway_trunk_seconds,

    service_hour,
    day_of_week,
    is_weekend,

    NOW()

FROM features


ON CONFLICT (
    service_date,
    trip_id,
    stop_sequence,
    stop_id
)

DO UPDATE SET

    route_id =
        EXCLUDED.route_id,

    direction_id =
        EXCLUDED.direction_id,

    vehicle_id =
        EXCLUDED.vehicle_id,

    trip_type =
        EXCLUDED.trip_type,

    scheduled_arrival =
        EXCLUDED.scheduled_arrival,

    scheduled_departure =
        EXCLUDED.scheduled_departure,

    observed_arrival =
        EXCLUDED.observed_arrival,

    observed_departure =
        EXCLUDED.observed_departure,

    arrival_deviation_seconds =
        EXCLUDED.arrival_deviation_seconds,

    departure_deviation_seconds =
        EXCLUDED.departure_deviation_seconds,

    travel_time_seconds =
        EXCLUDED.travel_time_seconds,

    dwell_time_seconds =
        EXCLUDED.dwell_time_seconds,

    headway_branch_seconds =
        EXCLUDED.headway_branch_seconds,

    headway_trunk_seconds =
        EXCLUDED.headway_trunk_seconds,

    service_hour =
        EXCLUDED.service_hour,

    day_of_week =
        EXCLUDED.day_of_week,

    is_weekend =
        EXCLUDED.is_weekend,

    updated_at = NOW();