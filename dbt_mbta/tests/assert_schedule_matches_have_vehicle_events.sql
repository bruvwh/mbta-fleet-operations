select
    matches.event_key,
    matches.event_ingestion_timestamp

from {{ ref('stg_vehicle_event_schedule_matches') }} as matches

left join {{ ref('stg_vehicle_events') }} as events

    on matches.event_key = events.event_key

   and matches.event_ingestion_timestamp
       = events.ingestion_timestamp

where events.event_key is null