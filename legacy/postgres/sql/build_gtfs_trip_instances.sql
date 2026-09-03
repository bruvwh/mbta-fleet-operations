INSERT INTO gtfs_trip_instances (
    feed_checksum,
    service_date,
    trip_id,
    route_id,
    service_id,
    direction_id
)

SELECT
    active.feed_checksum,
    active.service_date,

    trips.trip_id,
    trips.route_id,
    trips.service_id,
    trips.direction_id

FROM gtfs_active_services AS active

JOIN gtfs_trips AS trips
    ON active.feed_checksum = trips.feed_checksum
   AND active.service_id = trips.service_id

ON CONFLICT (
    feed_checksum,
    service_date,
    trip_id
)
DO NOTHING;