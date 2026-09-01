# MBTA Fleet Operations Engineering Notes

## Current Architecture

- Static GTFS batch ingestion
- Historical LAMP batch ingestion
- MBTA VehiclePositions polling every 5 seconds
- PostgreSQL operational warehouse
- Incremental SQL transformations using watermarks
- Historical IQR-based anomaly baselines
- Streamlit operational dashboard

## Realtime Pipeline Performance

### Before optimization

- Vehicle events: ~3.6 million
- Full realtime pipeline: 335.60 seconds

### After indexes

- Full realtime pipeline: 260.08 seconds

### After incremental transformations

Test batch:
- 4 new raw files
- 1,652 events seen
- 1,084 events inserted

Pipeline runtime:
- 52.35 seconds

Notable transformations:
- Schedule matching: 12.94 sec
- Stop inference: 1.50 sec
- Stop performance: 2.01 sec
- Trip summary: 0.16 sec
- Trip operations: 1.25 sec
- Coverage update: 2.45 sec
- Stop features: 10.79 sec

## Current Scalability Concerns

### Raw file growth

VehiclePositions is collected approximately every 5 seconds:

- ~720 snapshots/hour
- ~17,280 snapshots/day
- ~120,960 snapshots/week
- ~518,400 snapshots/month

Current loader output showed:

- 4 files processed
- 12,720 files skipped

Potential issue:
The loader may scan previously processed raw files every time it runs.

### Realtime table growth

Current vehicle_events:
- ~3.6 million rows

Long-running collection may result in tens or hundreds of millions
of vehicle observations.

Potential improvements to investigate:

- incremental raw-file discovery
- raw snapshot compaction
- PostgreSQL partitioning
- archival to Parquet
- orchestration
- long-term analytical storage

## Design Principle

Do not add infrastructure solely for resume value.

New technology should address a measured bottleneck or reliability
problem.

### Raw File Discovery Optimization

Before:
- 12,724 raw files discovered
- 12,724 previously processed files skipped
- loader runtime with no new data: 0.17 sec

After timestamp/partition-aware discovery:
- 12,724 raw files remained on disk
- only 4 recent candidate files inspected
- 4 previously processed files skipped
- file discovery time: <0.01 sec
- total no-data loader runtime: 0.06 sec

The loader now uses the latest processed ingestion timestamp plus a
10-minute safety lookback and filters candidate files using timestamped
date partitions rather than recursively scanning all historical raw files.

### Realtime Loader Backlog Benchmark

Test:
- 645 candidate snapshots
- 641 new snapshots
- 4 previously processed snapshots
- 170,981 source vehicle observations
- 82,691 unique vehicle events inserted
- loader runtime: 21.60 seconds

Throughput:
- ~7,916 source observations/sec
- ~3,828 unique inserts/sec
- ~48.4% of source observations produced new event rows

Approximately 53 minutes of 5-second snapshot collection was loaded
in 21.6 seconds, indicating that batched PostgreSQL ingestion currently
processes data substantially faster than the source generates it.