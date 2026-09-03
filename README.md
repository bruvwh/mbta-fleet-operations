# MBTA Fleet Operations Data Platform

An end-to-end cloud data platform for analyzing MBTA fleet operations using realtime vehicle telemetry, scheduled GTFS service, and historical LAMP subway data.

The platform collects MBTA GTFS-Realtime VehiclePositions, stores immutable raw snapshots in Google Cloud Storage, loads deduplicated vehicle events into BigQuery, transforms realtime and historical data with dbt and Spark, and produces stop-, trip-, and route-level operational metrics for a Streamlit dashboard.

## Project Goals

The project was designed around several practical data engineering problems:

- ingest frequently updating GTFS-Realtime vehicle data
- preserve raw source data for reproducibility and backfills
- handle observations repeated across consecutive realtime snapshots
- make ingestion idempotent
- version and match realtime observations against scheduled GTFS service
- infer stop arrival and departure behavior from noisy vehicle telemetry
- combine realtime service with historical operating patterns
- generate anomaly and reliability metrics at operationally useful grains
- support incremental transformation as the dataset grows
- orchestrate realtime and historical workloads independently

## Architecture

```text
                         MBTA APIs / Public Data
                                  |
             +--------------------+--------------------+
             |                                         |
             v                                         v
    GTFS-Realtime VehiclePositions              Historical LAMP
             |                                         |
             v                                         v
    Python GCS Collector                     Google Cloud Storage
             |                                         |
             v                                         v
    Google Cloud Storage                   Dataproc Serverless Spark
      raw protobuf snapshots                         |
             |                                       v
             v                                  BigQuery LAMP
    Python BigQuery Loader                           |
             |                                       |
             v                                       |
       BigQuery vehicle_events                       |
             |                                       |
             +-------------------+-------------------+
                                 |
                                 v
                                dbt
                                 |
             +-------------------+-------------------+
             |                   |                   |
             v                   v                   v
       Schedule Matching     Stop Inference    Historical Baselines
             |                   |                   |
             +-------------------+-------------------+
                                 |
                                 v
                       Stop Performance / Features
                                 |
                                 v
                          Anomaly Scoring
                                 |
                    +------------+------------+
                    |                         |
                    v                         v
              Trip-Level Marts          Route-Level Marts
                    |                         |
                    +------------+------------+
                                 |
                                 v
                     Streamlit Dashboard
```

## Technology Stack

### Cloud and Storage

- Google Cloud Platform
- Google Cloud Storage
- BigQuery
- Dataproc Serverless

### Data Engineering

- Python
- Apache Airflow 3
- Apache Spark
- dbt
- SQL
- GTFS-Realtime Protocol Buffers

### Analytics

- historical MBTA LAMP data
- scheduled GTFS data
- IQR-based historical anomaly baselines
- stop arrival/departure inference

### Presentation

- Streamlit

## Realtime Pipeline

The realtime collector polls the MBTA VehiclePositions feed approximately every five seconds.

```text
MBTA VehiclePositions
        |
        v
vehicle_positions_gcs.py
        |
        v
GCS raw protobuf snapshots
        |
        v
gcs_vehicle_events_to_bigquery.py
        |
        v
BigQuery vehicle_events
        |
        v
dbt realtime transformations
        |
        v
trip / route marts
```

Raw protobuf snapshots remain immutable in GCS so warehouse tables can be rebuilt without depending on the live MBTA endpoint.

Airflow runs the realtime cloud pipeline every five minutes.

## Idempotent Realtime Ingestion

GTFS-Realtime feeds may repeat an unchanged vehicle observation across consecutive snapshots.

Loading each snapshot row directly would therefore create duplicate logical events.

The platform creates a SHA-256 `event_key`.

When a vehicle timestamp is available:

```text
vehicle_id | vehicle_timestamp | trip_id
```

When it is unavailable:

```text
vehicle_id | feed_timestamp | trip_id | entity_id
```

The loader uses two independent deduplication layers.

### Python deduplication

Rows are grouped by `event_key` before BigQuery staging, with the earliest ingestion observation retained.

### BigQuery deduplication

The staging source is independently ranked:

```sql
ROW_NUMBER() OVER (
    PARTITION BY event_key
    ORDER BY
        ingestion_timestamp ASC,
        feed_timestamp ASC,
        entity_id ASC
)
```

Only rank 1 is presented to the BigQuery `MERGE`.

The final insert uses:

```text
MERGE ON event_key
```

This makes reruns and overlapping realtime snapshots idempotent.

### Validation

The live warehouse was validated with:

- manual loader execution
- repeated manual execution
- two consecutive scheduled Airflow runs
- approximately 8.8 million stored realtime vehicle events

Result:

```text
duplicate event_keys = 0
```

A benchmark batch containing 6,856 raw observations produced 4,017 unique event keys, eliminating 2,839 repeated snapshot observations before staging.

That batch removed approximately 41% of redundant realtime observations before warehouse insertion.

## Scheduled GTFS Matching

Realtime observations are matched to scheduled GTFS service using feed-version-aware schedule data.

The transformation layer accounts for:

- GTFS feed versions
- service dates
- trip IDs
- stop sequences
- local service time
- realtime event timestamps

The resulting schedule-match data supports downstream stop inference and service-performance calculations.

## Stop Inference

VehiclePositions does not directly provide complete arrival and departure events for every scheduled stop.

The realtime transformation layer therefore derives stop-level operational records from multiple vehicle observations.

Output includes:

- scheduled stop time
- inferred arrival bounds
- inferred departure bounds
- arrival and departure estimates
- observation counts
- number of observed vehicles
- inference uncertainty
- inference quality classification

The stop-event grain is:

```text
feed_checksum
service_date
trip_id
stop_sequence
stop_id
```

Validation found zero duplicate rows at this grain.

The current realtime dataset contains approximately 967,000 stop-level operational records.

## Historical LAMP Pipeline

Historical MBTA LAMP data is processed separately from the realtime pipeline.

```text
Historical LAMP files
        |
        v
Google Cloud Storage
        |
        v
Dataproc Serverless Spark
        |
        v
BigQuery lamp_stop_events
        |
        v
dbt historical transformations
        |
        v
historical stop baselines
```

Spark is used to process the larger historical source before loading normalized stop events into BigQuery.

Airflow orchestrates this historical pipeline independently of realtime ingestion.

## Historical Anomaly Baselines

Historical stop behavior is summarized using robust delay distributions.

Baseline fields include:

- first quartile delay
- median delay
- third quartile delay
- interquartile range
- lower anomaly fence
- upper anomaly fence

Realtime stop performance is then compared with the corresponding historical stop baseline.

Final classifications include:

```text
NORMAL
POSSIBLE_LATE
CERTAIN_LATE
NOT_EVALUABLE
```

The historical LAMP source primarily supports subway operations.

Across the subway routes used by the anomaly framework, approximately **94.35%** of realtime stop events had a usable historical baseline.

## dbt Transformation Layer

The dbt project contains staging, intermediate, and mart models.

### Staging

Examples:

```text
stg_vehicle_events
stg_vehicle_event_schedule_matches
stg_gtfs_trip_instances
stg_gtfs_stop_instances
stg_lamp_stop_events
```

### Intermediate

Examples:

```text
int_vehicle_event_schedule_matches_cloud
int_realtime_stop_events
int_realtime_stop_performance
int_realtime_stop_features
int_realtime_stop_anomaly_scores

int_historical_stop_events
int_historical_stop_performance
int_historical_stop_features
int_historical_baseline_primary
int_historical_baseline_fallback
```

### Marts

```text
mart_realtime_trip_anomalies
mart_realtime_route_hourly
mart_realtime_route_daily

mart_historical_stop_baselines
mart_historical_route_hourly
mart_historical_route_daily
```

## Current Warehouse Scale

Recent validation produced approximately:

| Dataset | Rows |
|---|---:|
| Realtime vehicle events | 8.8M |
| Realtime stop events | 968K |
| Realtime stop performance | 968K |
| Realtime stop features | 968K |
| Realtime anomaly scores | 968K |
| Realtime trip anomaly mart | 55K |
| Realtime route-hourly mart | 22K |
| Realtime route-daily mart | 2.4K |

Exact counts continue to increase while realtime collection runs.

## Performance Benchmarks

### GCS to BigQuery Loader

Representative incremental batch:

```text
GCS snapshots:                  9
Raw vehicle observations:  6,856
Unique event keys:         4,017
Repeated observations:     2,839
Wall-clock runtime:        23.14 sec
```

Most wall-clock time is cloud I/O and BigQuery job latency rather than local CPU execution.

### Incremental dbt Realtime Branch

A nine-model incremental BigQuery transformation run completed in:

```text
dbt execution time: 66.04 sec
wall-clock time:    68.99 sec
status:             9 PASS / 0 ERROR
```

The run included schedule matching, stop inference, performance calculation, feature engineering, anomaly scoring, and operational marts.

## Data Quality

The project includes dbt tests and standalone profiling tools covering:

- schedule-match uniqueness
- schedule-match validity
- realtime stop-event uniqueness
- realtime stop-event validity
- anomaly-score validity
- historical baseline validity
- historical baseline grain uniqueness
- historical route mart uniqueness
- LAMP staging uniqueness
- vehicle-event schedule consistency

The full dbt test suite passes.

## Dashboard

The Streamlit dashboard queries BigQuery directly.

It provides operational views built from the final realtime marts and anomaly tables, including:

- route-level reliability
- stop-level delay behavior
- anomaly classifications
- historical baseline comparisons
- service-date filtering
- subway route filtering

The dashboard runtime does not require PostgreSQL.

Run locally with:

```bash
streamlit run src/dashboard/app_bigquery.py
```

## Airflow

Two cloud DAGs orchestrate the platform:

```text
mbta_realtime_cloud_pipeline
mbta_historical_cloud_pipeline
```

The realtime DAG runs every five minutes.

For local Airflow execution:

```bash
cd /Users/andrewlu/Desktop/mbta-fleet-operations

source .airflow-venv/bin/activate

export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"

airflow standalone
```

The `AIRFLOW__CORE__DAGS_FOLDER` override is required because the repository keeps DAG definitions in the top-level `dags/` directory rather than under `.airflow/dags`.

## Repository Structure

```text
mbta-fleet-operations/
|
├── dags/
│   ├── mbta_realtime_cloud_pipeline.py
│   └── mbta_historical_cloud_pipeline.py
│
├── dbt_mbta/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
│
├── src/
│   ├── dashboard/
│   ├── ingestion/
│   ├── loading/
│   ├── quality/
│   └── spark/
│
├── docs/
│   └── engineering_notes.md
│
├── legacy/
│   └── postgres/
│
├── README.md
├── requirements.txt
└── docker-compose.yml
```

## Legacy PostgreSQL Implementation

The project originally used a local PostgreSQL warehouse and Python/SQL transformation pipeline.

That implementation is preserved under:

```text
legacy/postgres/
```

It includes earlier work on:

- local realtime ingestion
- PostgreSQL indexing
- incremental SQL transformations
- local historical loading
- local Streamlit analytics
- pipeline runtime optimization

The production architecture was later migrated to GCS, BigQuery, dbt, Dataproc Serverless, and cloud-oriented Airflow orchestration.

The legacy implementation is retained to document the engineering evolution of the project, but it is not required by the current cloud realtime or historical pipelines.

## Engineering Principles

This project intentionally avoids adding infrastructure solely for technology breadth.

Tools were introduced when they addressed a concrete requirement:

- GCS for immutable raw storage and replayability
- BigQuery for scalable analytical storage
- dbt for warehouse transformation and testing
- Spark for historical LAMP processing
- Airflow for independent realtime and historical orchestration
- Streamlit for presenting operational metrics

The architecture evolved in response to measured scalability, reliability, and maintainability problems rather than as a collection of disconnected technologies.