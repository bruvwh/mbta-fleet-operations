-- ============================================================
-- Migration 002: Pipeline run logging
--
-- One row represents one execution of a pipeline.
--
-- This provides basic observability for:
--   - when a pipeline started
--   - whether it succeeded or failed
--   - how long it took
--   - how many events were inserted
--   - what error occurred if it failed
-- ============================================================


CREATE TABLE IF NOT EXISTS pipeline_runs (

    run_id BIGSERIAL PRIMARY KEY,

    pipeline_name TEXT NOT NULL,

    started_at TIMESTAMPTZ
        NOT NULL
        DEFAULT NOW(),

    finished_at TIMESTAMPTZ,

    status TEXT
        NOT NULL
        DEFAULT 'RUNNING',

    duration_seconds DOUBLE PRECISION,

    events_inserted BIGINT,

    error_message TEXT,

    created_at TIMESTAMPTZ
        NOT NULL
        DEFAULT NOW(),


    CHECK (
        status IN (
            'RUNNING',
            'SUCCESS',
            'FAILED'
        )
    ),

    CHECK (
        duration_seconds IS NULL
        OR duration_seconds >= 0
    ),

    CHECK (
        events_inserted IS NULL
        OR events_inserted >= 0
    )
);


CREATE INDEX IF NOT EXISTS
    idx_pipeline_runs_pipeline_started

ON pipeline_runs (
    pipeline_name,
    started_at DESC
);


CREATE INDEX IF NOT EXISTS
    idx_pipeline_runs_status

ON pipeline_runs (
    status,
    started_at DESC
);