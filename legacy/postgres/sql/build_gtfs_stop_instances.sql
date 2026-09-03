INSERT INTO gtfs_stop_instances (
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence,
    stop_id,

    route_id,
    direction_id,

    scheduled_arrival_local,
    scheduled_departure_local,

    arrival_seconds,
    departure_seconds
)

SELECT
    ti.feed_checksum,
    ti.service_date,
    ti.trip_id,

    st.stop_sequence,
    st.stop_id,

    ti.route_id,
    ti.direction_id,

    CASE
        WHEN st.arrival_seconds IS NOT NULL
        THEN
            ti.service_date::timestamp
            + st.arrival_seconds * INTERVAL '1 second'
        ELSE NULL
    END AS scheduled_arrival_local,

    CASE
        WHEN st.departure_seconds IS NOT NULL
        THEN
            ti.service_date::timestamp
            + st.departure_seconds * INTERVAL '1 second'
        ELSE NULL
    END AS scheduled_departure_local,

    st.arrival_seconds,
    st.departure_seconds

FROM gtfs_trip_instances ti

JOIN gtfs_stop_times st
  ON ti.feed_checksum = st.feed_checksum
 AND ti.trip_id = st.trip_id

ON CONFLICT (
    feed_checksum,
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

    scheduled_arrival_local =
        EXCLUDED.scheduled_arrival_local,

    scheduled_departure_local =
        EXCLUDED.scheduled_departure_local,

    arrival_seconds =
        EXCLUDED.arrival_seconds,

    departure_seconds =
        EXCLUDED.departure_seconds;