{{ config(
    materialized='view'
) }}


with legacy_matches as (

    select
        event_key,

        event_ingestion_timestamp,

        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        cast(
            scheduled_local_time
            as datetime
        ) as scheduled_local_time,

        cast(
            difference_seconds
            as float64
        ) as difference_seconds,

        created_at,

        'POSTGRES_LEGACY'
            as match_source,

        1 as source_priority

    from {{ source(
        'mbta',
        'vehicle_event_schedule_matches'
    ) }}

),


cloud_matches as (

    select
        event_key,

        event_ingestion_timestamp,

        feed_checksum,

        service_date,

        trip_id,

        stop_sequence,

        cast(
            scheduled_local_time
            as datetime
        ) as scheduled_local_time,

        cast(
            difference_seconds
            as float64
        ) as difference_seconds,

        created_at,

        'BIGQUERY_CLOUD'
            as match_source,

        2 as source_priority

    from {{ ref(
        'int_vehicle_event_schedule_matches_cloud'
    ) }}

),


combined as (

    select *
    from legacy_matches

    union all

    select *
    from cloud_matches

),


deduplicated as (

    select
        *,

        row_number() over (

            partition by
                event_key

            order by
                source_priority,
                created_at desc

        ) as source_rank

    from combined

)


select
    event_key,

    event_ingestion_timestamp,

    feed_checksum,

    service_date,

    trip_id,

    stop_sequence,

    scheduled_local_time,

    difference_seconds,

    created_at,

    match_source

from deduplicated

where source_rank = 1