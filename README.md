Artemis
=======

A local F1 data pipeline that ingests race data from the OpenF1 API into ClickHouse,
transforms it with dbt, and visualises it in Grafana.

The pipeline currently covers the 2026 season. Each run fetches meetings, sessions,
drivers, laps and stints, validates the raw payloads, then runs dbt staging,
intermediate and mart models to produce a star-schema fact table and four analytical
mart tables.

## Stack

| Component | Version | Role |
|-----------|---------|------|
| Apache Airflow | 3.0.6 | Orchestration |
| Astronomer Cosmos | 1.14 | dbt-in-Airflow integration |
| dbt-core + dbt-clickhouse | 1.11 / 1.10 | Transformations |
| ClickHouse | 24.1 | Analytical store |
| Grafana | 11.3 | Dashboards |
| Prometheus + StatsD | 2.53 / 0.28 | Airflow metrics |
| Elasticsearch + Kibana | 8.17 | Log aggregation |
| Vault | 1.18 | Secrets (optional) |

## Prerequisites

- Docker and Docker Compose
- [uv](https://github.com/astral-sh/uv) (Python dependency management)

## Running locally

```bash
# Start the full stack
make launch-stack

# Create the Airflow pool used by F1 tasks (run once after first launch)
make f1-pool
```

Services:

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| Grafana | http://localhost:3000 | admin / admin |
| ClickHouse HTTP | http://localhost:8123 | default / test123 |
| Kibana | http://localhost:5601 | |
| Prometheus | http://localhost:9090 | |

To tear down:

```bash
make teardown          # stop containers, keep volumes
make teardown-all      # stop containers and remove all volumes
```

## DAG

The `f1` DAG runs once on trigger. The task sequence is:

```
setup_raw_tables
  -> ingest_meetings  -> validate_meetings
  -> ingest_sessions  -> validate_sessions
  -> ingest_drivers   -> validate_drivers
  -> ingest_laps      -> validate_laps
  -> ingest_stints    -> validate_stints
       -> all_validated
            -> staging_f1 (dbt)
                 -> intermediate_f1 (dbt)
                      -> marts_f1 (dbt)
```

Ingestion pulls from the [OpenF1 REST API](https://openf1.org) and writes raw JSON
payloads to `raw.*` tables in ClickHouse. Validation checks row counts and basic
constraints before any transformation runs.

## dbt models

```
models/
  staging/        # typed, renamed columns from raw tables
  intermediate/   # int_laps_with_stint: joins laps to stints, adds tyre_age
  marts/          # fct_laps, mart_driver_pace, mart_compound_pace, mart_degradation
```

Run dbt parse locally:

```bash
make dbt-parse
```

## Development

```bash
make lint        # ruff check + format
make typecheck   # pyright
```

Dependencies are managed with uv. After cloning:

```bash
uv sync
```
