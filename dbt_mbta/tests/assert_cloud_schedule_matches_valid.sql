select *

from {{ ref(
    'int_vehicle_event_schedule_matches_cloud'
) }}

where

    event_key is null

    or event_ingestion_timestamp is null

    or feed_checksum is null

    or service_date is null

    or trip_id is null

    or difference_seconds is null

    or difference_seconds < 0