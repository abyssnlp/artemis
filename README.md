Artemis
=======

Artemis is a local Airflow and dbt project for building an F1 analytics pipeline.
It ingests data from the OpenF1 API into ClickHouse, validates the raw payloads,
models the data with dbt, and exposes Airflow and F1 dashboards through Grafana.

The project is intended for local development. The Docker Compose stack includes
Airflow, ClickHouse, dbt support through Astronomer Cosmos, Grafana, Prometheus,
StatsD Exporter, Elasticsearch, Kibana, Filebeat, Vault, PostgreSQL, and Redis.

## What is included

- Custom Airflow operators for OpenF1 ingestion and Pydantic payload validation.
- A primary `f1` DAG that runs ingestion, validation, and dbt models in one flow.
- Asset-based DAGs that split ingestion, staging/intermediate models, and marts.
- A dbt project under `dbt/f1` with staging, intermediate, fact, and mart models.
- ClickHouse configuration for local analytics storage.
- Grafana provisioning for Prometheus and ClickHouse data sources.
- Airflow metrics collection through StatsD Exporter and Prometheus.
- Airflow log shipping through Filebeat, Elasticsearch, and Kibana.
- Vault initialization for Airflow connections and variables used by the stack.

## Stack

| Component | Version | Role |
|-----------|---------|------|
| Apache Airflow | 3.0.6 | Workflow orchestration |
| Astronomer Cosmos | 1.14 or newer | dbt task groups in Airflow |
| dbt-core | 1.11 or newer | SQL transformation framework |
| dbt-clickhouse | 1.10 or newer | ClickHouse adapter for dbt |
| ClickHouse | 24.1 | Analytical database |
| Grafana | 11.3 | Dashboards |
| Prometheus | 2.53 | Metrics storage |
| StatsD Exporter | 0.28 | Airflow metrics bridge |
| Elasticsearch and Kibana | 8.17 | Log indexing and log search |
| Filebeat | 8.17 | Airflow log shipping |
| Vault | 1.18 | Airflow secrets backend |
| PostgreSQL | 13 | Airflow metadata database |
| Redis | 7.2 | Celery broker |

## Prerequisites

- Docker and Docker Compose
- `uv` for Python dependency management

Install Python dependencies for local linting, type checking, and dbt parsing:

```bash
uv sync
```

## Running the stack

Start all local services:

```bash
make launch-stack
```

Build the Airflow image while starting the stack:

```bash
make launch-stack BUILD=1
```

The Airflow initialization container creates the `f1` pool automatically. If you
need to recreate it manually, run:

```bash
make f1-pool
```

Stop containers while keeping volumes:

```bash
make teardown
```

Stop containers and remove volumes:

```bash
make teardown-all
```

## Local services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| ClickHouse HTTP | http://localhost:8123 | `default` / `test123` |
| ClickHouse native | `localhost:9000` | `default` / `test123` |
| Kibana | http://localhost:5601 | none |
| Prometheus | http://localhost:9090 | none |
| StatsD Exporter | http://localhost:9102 | none |
| Vault | http://localhost:8200 | token managed in the Vault volume |

## Airflow setup

Vault is configured as the Airflow secrets backend. The `vault-init` service
initializes and unseals Vault, creates an `airflow` token, and writes these
Airflow connections:

- `clickhouse_default`
- `postgres_default`
- `redis_default`

It also writes the `airflow_env` variable used by the sample `hello_world` DAG.

The F1 ingestion operator reads an Airflow variable named `f1`. Create it before
triggering the `f1` or `f1_with_asset` DAGs:

```bash
docker compose exec airflow-apiserver airflow variables set f1 \
  '{"f1_meeting_key": 1254, "f1_session_key": 9977}'
```

Use meeting and session keys from OpenF1 for the session you want to ingest. The
current ingestion code requests 2026 meetings and uses the configured meeting
and session keys for session-level endpoints.

## DAGs

| DAG | Purpose |
|-----|---------|
| `f1` | Main end-to-end pipeline. Ingests raw OpenF1 data, validates it, then runs dbt staging, intermediate, and mart models. |
| `f1_with_asset` | Ingests and validates OpenF1 data, then emits Airflow assets for the raw tables. |
| `staging_intermediate_asset` | Runs dbt staging and intermediate models when raw table assets are updated. |
| `marts_asset` | Runs dbt mart models after the intermediate asset is updated. |
| `hello_world` | Small sample DAG that reads the `airflow_env` variable. |
| `example_cosmos` | Cosmos example DAG kept for experimentation. It selects paths that are not part of the current dbt model tree. |

The primary `f1` DAG flow is:

```text
test_start
  -> setup_raw_tables
  -> ingest_meetings  -> validate_meetings
  -> ingest_sessions  -> validate_sessions
  -> ingest_drivers   -> validate_drivers
  -> ingest_laps      -> validate_laps
  -> ingest_stints    -> validate_stints
  -> all_validated
  -> staging_f1
  -> intermediate_f1
  -> marts_f1
  -> test_end
```

Raw data is stored in ClickHouse under the `raw` database. Each raw table stores
the original JSON payload plus source endpoint, ingestion timestamp, and Airflow
run ID. Inserts use a temporary table and `EXCHANGE TABLES` so each endpoint load
replaces the previous table contents atomically.

Validation uses Pydantic schemas for these OpenF1 endpoints:

- `meetings`
- `sessions`
- `drivers`
- `laps`
- `stints`

The current DAGs run validation with `strict=False`, so validation failures are
logged and counted without failing the task unless the configured failure
threshold is reached.

## dbt project

The dbt project is in `dbt/f1`.

```text
dbt/f1/models
  staging
    stg_meetings
    stg_sessions
    stg_drivers
    stg_laps
    stg_stints
  intermediate
    int_laps_with_stint
  marts
    fct_laps
    mart_driver_pace
    mart_compound_pace
    mart_degradation
```

The staging models parse raw JSON from ClickHouse into typed columns. The
intermediate model joins laps to stints and calculates tyre age. The mart layer
creates a lap-grain fact table and pace/degradation aggregates for analysis.

Run dbt parsing locally after dependencies are installed:

```bash
make dbt-parse
```

The local dbt profile in `dbt/f1/profiles.yml` points to ClickHouse on
`localhost:8123`. Airflow uses the `clickhouse_default` connection from Vault and
mounts the dbt project at `/opt/airflow/dbt`.

## Development commands

Run Ruff checks and formatting:

```bash
make lint
```

Run Pyright:

```bash
make typecheck
```

## Repository layout

```text
artemis/
  dags/
    common/components/      Custom ingestion and validation operators
    common/utils/           Cosmos and dbt helper configuration
    f1.py                   Main end-to-end DAG
    f1_with_asset.py        Ingestion DAG with Airflow asset outlets
    staging_intermediate_asset.py
    marts_asset.py
    hello_world.py
    example_cosmos.py
clickhouse/config/          Local ClickHouse server configuration
dbt/f1/                     dbt project for F1 transformations
monitoring/
  filebeat/                 Airflow log shipper config
  grafana/                  Dashboards and data source provisioning
  kibana/                   Kibana setup script
  prometheus/               Prometheus scrape config
  statsd/                   StatsD mapping for Airflow metrics
vault/                      Vault server and initialization config
Dockerfile                  Airflow image with project dependencies
docker-compose.yaml         Local service stack
Makefile                    Common local commands
pyproject.toml              Python project and tool configuration
constraints-3.12.txt        Airflow dependency constraints
uv.lock                     Locked Python dependencies
```

## Notes

- The stack is for local development only.
- `make teardown-all` removes local service volumes, including ClickHouse,
  PostgreSQL, Grafana, Prometheus, Elasticsearch, and Vault data.
- Grafana starts with provisioned Prometheus and ClickHouse data sources plus
  Airflow and F1 dashboards.
- Kibana and Filebeat require access to local Docker container logs through the
  mounted Docker paths in `docker-compose.yaml`.
