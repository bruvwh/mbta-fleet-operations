select
    event_key,
    count(*) as row_count

from {{ ref(
    'int_vehicle_event_schedule_matches_cloud'
) }}

group by
    event_key

having count(*) > 1