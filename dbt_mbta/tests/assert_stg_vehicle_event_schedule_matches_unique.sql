select
    event_key,
    count(*) as row_count

from {{ ref(
    'stg_vehicle_event_schedule_matches'
) }}

group by
    event_key

having count(*) > 1