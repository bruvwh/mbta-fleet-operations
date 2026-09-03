{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=[
        'feed_checksum',
        'service_date',
        'trip_id'
    ],
    partition_by={
        "field": "service_date",
        "data_type": "date"
    },
    cluster_by=[
        "route_id",
        "trip_id"
    ]
) }}


with affected_trips as (

    select distinct
        feed_checksum,
        service_date,
        trip_id

    from {{ ref(
        'int_realtime_stop_anomaly_scores'
    ) }}


    {% if is_incremental() %}

    where updated_at >= timestamp_sub(

        coalesce(

            (
                select
                    max(updated_at)

                from {{ this }}
            ),

            timestamp(
                '1970-01-01 00:00:00+00'
            )
        ),

        interval 30 minute
    )

    {% endif %}

),


affected_scores as (

    select
        scores.*

    from {{ ref(
        'int_realtime_stop_anomaly_scores'
    ) }} as scores

    join affected_trips as affected

      on scores.feed_checksum
            = affected.feed_checksum

     and scores.service_date
            = affected.service_date

     and scores.trip_id
            = affected.trip_id

),


trip_summary as (

    select
        feed_checksum,

        service_date,

        trip_id,

        any_value(
            route_id
        ) as route_id,

        any_value(
            direction_id
        ) as direction_id,


        count(*)
            as inferred_stop_count,


        countif(
            stop_classification
                != 'NOT_EVALUABLE'
        ) as observed_stop_count,


        countif(
            is_anomalous
        ) as anomalous_stop_count,


        countif(
            stop_classification
                = 'CERTAIN_LATE'
        ) as certain_late_stop_count,


        countif(
            stop_classification
                = 'POSSIBLE_LATE'
        ) as possible_late_stop_count,


        countif(
            stop_classification
                = 'NORMAL'
        ) as normal_stop_count,


        100.0
        * safe_divide(

            countif(
                is_anomalous
            ),

            countif(
                stop_classification
                    != 'NOT_EVALUABLE'
            )

        ) as anomaly_percent,


        avg(

            if(
                stop_classification
                    != 'NOT_EVALUABLE',

                arrival_deviation_seconds,

                null
            )

        ) as avg_arrival_deviation_seconds

    from affected_scores

    group by
        feed_checksum,
        service_date,
        trip_id

),


classified as (

    select
        *,


        case

            when observed_stop_count < 5
                then 'INSUFFICIENT_COVERAGE'


            when
                anomalous_stop_count >= 3

                and anomaly_percent >= 50

            then 'SUSTAINED_LATE'


            else 'NORMAL'

        end as trip_classification,


        current_timestamp()
            as updated_at

    from trip_summary

)


select *

from classified