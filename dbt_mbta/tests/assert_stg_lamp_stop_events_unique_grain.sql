select
    service_date,
    trip_id,
    stop_sequence,
    stop_id,
    count(*) as row_count

from {{ ref('stg_lamp_stop_events') }}

group by
    service_date,
    trip_id,
    stop_sequence,
    stop_id

having count(*) > 1