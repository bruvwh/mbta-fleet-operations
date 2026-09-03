select *

from {{ ref('int_realtime_stop_events') }}

where

    inference_quality not in (
        'MULTIPLE_VEHICLES',
        'NO_STOP_OBSERVATION',
        'MULTIPLE_STOP_PERIODS',
        'COMPLETE',
        'PARTIAL'
    )

    or arrival_uncertainty_seconds < 0

    or departure_uncertainty_seconds < 0