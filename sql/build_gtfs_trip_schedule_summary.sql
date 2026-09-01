INSERT INTO gtfs_trip_schedule_summary (
    feed_checksum,
    service_date,
    trip_id,
    scheduled_start_local,
    scheduled_end_local,
    scheduled_stop_count
)

SELECT
    instances.feed_checksum,
    instances.service_date,
    instances.trip_id,

    instances.service_date::timestamp
        + (
            MIN(
                COALESCE(
                    stop_times.departure_seconds,
                    stop_times.arrival_seconds
                )
            )
            * INTERVAL '1 second'
        ) AS scheduled_start_local,

    instances.service_date::timestamp
        + (
            MAX(
                COALESCE(
                    stop_times.arrival_seconds,
                    stop_times.departure_seconds
                )
            )
            * INTERVAL '1 second'
        ) AS scheduled_end_local,

    COUNT(*) AS scheduled_stop_count

FROM gtfs_trip_instances AS instances

JOIN gtfs_stop_times AS stop_times
    ON instances.feed_checksum = stop_times.feed_checksum
   AND instances.trip_id = stop_times.trip_id

GROUP BY
    instances.feed_checksum,
    instances.service_date,
    instances.trip_id

ON CONFLICT (
    feed_checksum,
    service_date,
    trip_id
)

DO UPDATE SET
    scheduled_start_local = EXCLUDED.scheduled_start_local,
    scheduled_end_local = EXCLUDED.scheduled_end_local,
    scheduled_stop_count = EXCLUDED.scheduled_stop_count;