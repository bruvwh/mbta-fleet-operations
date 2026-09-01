INSERT INTO gtfs_active_services (
    feed_checksum,
    service_date,
    service_id
)

SELECT
    feeds.feed_checksum,
    dates.service_date,
    services.service_id

FROM gtfs_feeds AS feeds

CROSS JOIN LATERAL (

    SELECT
        generate_series(
            feeds.feed_start_date,
            feeds.feed_end_date,
            INTERVAL '1 day'
        )::date AS service_date

) AS dates

CROSS JOIN LATERAL active_gtfs_services(
    feeds.feed_checksum,
    dates.service_date
) AS services

WHERE feeds.schedule_loaded_at IS NOT NULL

ON CONFLICT (
    feed_checksum,
    service_date,
    service_id
)
DO NOTHING;