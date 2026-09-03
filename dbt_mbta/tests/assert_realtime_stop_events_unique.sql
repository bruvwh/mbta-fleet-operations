select
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence,
    stop_id,

    count(*) as row_count

from {{ ref('int_realtime_stop_events') }}

group by
    feed_checksum,
    service_date,
    trip_id,
    stop_sequence,
    stop_id

having count(*) > 1