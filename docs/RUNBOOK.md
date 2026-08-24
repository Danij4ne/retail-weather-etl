# Operations Runbook

Operational guide to diagnose and recover common failures in the `retail-weather-etl` pipeline.

## Current architecture

- Python
- DuckDB
- Airflow
- Docker Compose
- Parquet
- Open-Meteo API

## Ownership

- Owner: Data Engineering
- Current alert path: local logs + Airflow UI + optional Slack webhook

## Scope

This runbook covers:

- local pipeline execution
- Airflow + Docker Compose execution
- `extract`, `transform`, `validations`, and `load` failures
- common issues around inputs, weather API, quality gate, and DuckDB

It does not cover:

- enterprise-grade secret recovery or cloud infrastructure
- external warehouse restoration
- multi-user or out-of-scope deployments

## Threat model (local runtime)

This stack is local-first and single-user. The host is the security perimeter, not the containers.

- Postgres is not published to the host. It has no `ports:` mapping; only the Airflow services on the Compose network can reach it.
- The Airflow UI binds to `127.0.0.1:8080` on the host. It is not intended to be reachable from other machines on the LAN. Codespace or SSH port-forward still targets that localhost bind.
- Secrets live in `.env` (gitignored), not in `docker-compose.yml`. Compose interpolates `POSTGRES_*` and `AIRFLOW_ADMIN_PASSWORD` at start time; a missing admin password fails `airflow-init` instead of falling back to `admin`. An existing `.env` created before those Postgres keys existed must be updated before `airflow-start`.
- The full project bind mount (`../:/opt/airflow/project`) is intentional: `warehouse.duckdb`, `logs/etl/lineage/`, and `tmp/staging/airflow/` live under the project root. Narrower mounts would hide those artifacts from the operator.
- This is not a multi-user Airflow deployment. Do not treat Compose as production hardening.

## Airflow image dependencies

The project uses `uv` locally, in CI, and in the ETL image. `docker/Dockerfile.airflow` uses `pip` because `apache/airflow` only puts the `airflow` user's site-packages on `sys.path` (`PIP_USER`). `uv` does not honor that, so packages would be invisible to the interpreter.

Unused Google and Snowflake providers are uninstalled in that image because they pin `pandas<2.2`, which the ETL requirements outgrow. `pip check` guards the resolution. Do not switch this Dockerfile to `uv` without changing how the base image exposes site-packages.

## Incident severity

| Severity | Description                                                                                                                                  |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Low      | Reproducible local failure with no impact on published outputs or scheduled Airflow execution.                                               |
| Medium   | DAG, scheduler, or recurring execution failure that requires operator intervention to restore flow.                                          |
| High     | Data corruption/inconsistency, repeated `QualityGateError` without controlled root cause, or invalid/missing `gold` outputs after rerun. |

## Quick triage table

| Failure                  | Look first                         | Immediate action                                                       |
| ------------------------ | ---------------------------------- | ---------------------------------------------------------------------- |
| Weather API outage       | `logs/etl/pipeline.log`          | Retry; if a valid `weather_daily.csv` exists, run offline.           |
| `QualityGateError`     | `quality_report` in logs/lineage | Review `critical_failures` and `sales_rejected.csv`.               |
| Airflow does not start   | `docker compose ... logs`        | `make airflow-down` -> `make airflow-init` -> `make airflow-up`. |
| Compose build: `error getting credentials` | Docker Desktop / Hub login | `docker pull apache/airflow:2.10.5` then `make airflow-start`. |
| Airflow logs permission error | `airflow-init` traceback | `sudo chown -R "$(id -u):$(id -g)" logs` -> `make airflow-start`. |
| `load` fails           | SQL +`warehouse.duckdb`          | Run `make test-integration` and inspect exports.                     |
| Missing `gold` outputs | lineage `artifacts`              | Confirm active formats and inspect the `load` stage.                 |

## Initial triage checklist

Before changing anything:

1. Capture context:
   - command executed
   - local run or Airflow run
   - approximate time
   - latest config/repo change
2. Check `logs/etl/pipeline.log` and search for:
   - `=== PIPELINE FAILED ===`
   - `=== PIPELINE FAILED (QUALITY GATE) ===`
   - `[QUALITY]`
   - `[LINEAGE]`
3. Inspect the latest lineage file in `logs/etl/lineage/lineage_<run_id>.json`.
4. Identify the failing stage:
   - `extract_weather`
   - `extract_csv`
   - `profiling`
   - `transform`
   - `load`
5. Do not delete `silver`/`gold` outputs or `warehouse.duckdb` until the error is understood.

## Useful commands

Activate local environment:

```bash
uv sync --locked
```

Run full pipeline:

```bash
uv run python -m pipelines.pipeline
```

Run pipeline offline:

```bash
uv run python -c "from pipelines.pipeline import run_pipeline; run_pipeline(fetch_weather=False)"
```

Quick verification tests:

```bash
make test-unit
make test-integration
```

Airflow status/logs:

```bash
cd docker
docker compose --env-file ../.env ps
docker compose --env-file ../.env logs airflow-webserver
docker compose --env-file ../.env logs airflow-scheduler
```

Airflow reuses the existing `data/raw/weather_daily.csv` by default.
Set `RETAIL_ETL_FETCH_WEATHER=true` in `.env` only when you want the DAG `extract` task to refresh weather from the API.

Quick DuckDB inspection:

```bash
duckdb warehouse.duckdb
```

Useful DuckDB queries:

```sql
SHOW TABLES;
SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM weather_sales_mart;
SELECT * FROM weather_sales_mart LIMIT 5;
```

## Scenario 1: Open-Meteo API fails

### Symptoms

- failure in `extract_weather`
- timeouts, `429`, `5xx`, or connection errors
- `data/raw/weather_daily.csv` is not refreshed

### Check

1. `logs/etl/pipeline.log`
2. Airflow `extract` task logs if applicable
3. timestamp and contents of `data/raw/weather_daily.csv`

### Immediate action

1. Retry once.
2. If continuity matters and a valid `weather_daily.csv` already exists, run offline:

```bash
uv run python -c "from pipelines.pipeline import run_pipeline; run_pipeline(fetch_weather=False)"
```

3. If the issue persists, treat it as an upstream dependency outage.

### When to stop and escalate

- after repeated `429`/`5xx`
- when no valid local `weather_daily.csv` exists and offline execution is not possible

## Scenario 2: `QualityGateError`

### Symptoms

- the run fails during `transform`
- logs include `[QUALITY] failed_report=...`
- Airflow marks `transform` as failed

### Check

1. `logs/etl/pipeline.log`
2. `logs/etl/lineage/lineage_<run_id>.json`
3. `data/processed/rejected/sales_rejected.csv`
4. `critical_failures` and `metrics` in the `quality_report`

### Quick interpretation

- `silver` contract failure: invalid columns/dtypes/duplicates before SQL
- referential failure: orphan ids or inconsistent keys
- business-quality failure: coverage/integrity below acceptable threshold

### Action

1. Read `critical_failures` and identify the affected dataset.
2. If the problem comes from input data, fix `data/raw/*.csv` or the configuration.
3. If the problem comes from cleaning rules, inspect:
   - `etl/transform_parts/clean_sales.py`
   - `etl/transform_parts/clean_customers.py`
   - `etl/transform_parts/clean_products.py`
   - `etl/transform_parts/clean_weather.py`
4. Re-run locally before triggering Airflow again.

### Do not

- do not force `load` after a quality gate failure
- do not delete `sales_rejected.csv` before reviewing `reject_reasons`

## Scenario 3: raw input file is missing or misnamed

### Symptoms

- `FileNotFoundError` during `extract_csv`
- missing `customers.csv`, `products.csv`, `sales.csv`, or `weather_daily.csv`

### Check

1. `data/raw/`
2. active config in `config/config.yaml`
3. override via `RETAIL_ETL_CONFIG`

### Action

1. Verify the expected files exist.
2. If only the weather file is affected and continuity is needed, use `fetch_weather=False` only when `weather_daily.csv` already exists.
3. If paths were renamed, confirm `get_settings()` is reading the intended YAML.

## Scenario 4: `load` / SQL / DuckDB fails

### Symptoms

- failure in `load`
- invalid SQL, missing table, or export failure
- `warehouse.duckdb` is missing or inconsistent

### Check

1. `logs/etl/pipeline.log`
2. SQL path in `config/config.yaml` (`paths.sql_model`)
3. `warehouse.duckdb`
4. outputs under:
   - `data/processed/gold/parquet/`
   - `data/processed/gold/csv/` if CSV was enabled

### Action

1. Confirm the SQL file exists and ends with `.sql`.
2. Run load-focused integration tests:

```bash
make test-integration
```

3. If local artifacts are suspected to be corrupted, delete them only after diagnosis:
   - `warehouse.duckdb`
   - `gold` outputs
4. Re-run the pipeline.

### When to escalate

- if the SQL compiles but breaks model invariants
- if the failure reproduces in integration tests

## Scenario 5: Airflow does not start or remains unstable

### Symptoms

- `airflow-webserver` or `airflow-scheduler` does not come up
- UI at `localhost:8080` does not respond
- `airflow-init` fails
- `make airflow-start` dies during image build with `error getting credentials` while loading `apache/airflow`

### Check

```bash
cd docker
docker compose --env-file ../.env ps
docker compose --env-file ../.env logs airflow-webserver
docker compose --env-file ../.env logs airflow-scheduler
docker compose --env-file ../.env logs airflow-init
```

### Action

1. Verify `.env` exists, includes `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` and `AIRFLOW_ADMIN_PASSWORD`, and `AIRFLOW_UID` matches your `id -u` (`make env-init` sets it on a new `.env`; an older file must be updated by hand).
2. If the build fails with `error getting credentials` (empty helper output) while resolving `apache/airflow:2.10.5`, that is Docker Desktop's credential helper, not the Dockerfile. Compose/BuildKit treats a helper failure as fatal even for a public image. Pull with the CLI (it can fall back to an anonymous pull), then retry:

```bash
docker pull apache/airflow:2.10.5
make airflow-start
```

If the pull also fails, sign out of Docker Hub in Docker Desktop or run `docker logout`, then pull again.
3. If Postgres rejects Airflow after changing `POSTGRES_PASSWORD`, the named volume still has the old credentials. From `docker/`: `docker compose --env-file ../.env down -v` (this drops the Postgres volume, not silver/gold Parquet) then `make airflow-start`.
4. If `airflow-init` fails with `Permission denied` on `/opt/airflow/logs`, the mounted `logs/` is root-owned:

```bash
sudo chown -R "$(id -u):$(id -g)" logs
```

5. Run basic recovery:

```bash
make airflow-down
make airflow-init
make airflow-up
```

6. If image or dependency issues are suspected:

```bash
make airflow-build
make airflow-check
```

### Notes

- this repo assumes workers can access the same `PROJECT_ROOT`
- the DAG uses mounted-volume staging, not `/tmp`

## Scenario 6: a specific DAG task fails

### Symptoms

- the DAG loads, but one task fails (`extract`, `transform`, `load`)

### Check

1. task logs in Airflow UI
2. `logs/etl/pipeline.log` or intermediate artifacts when applicable
3. minimal XCom metadata
4. lineage JSON persisted by `cleanup`

### Action by task

- `extract`: inspect staging paths and weather API behavior
- `transform`: inspect `validation_report` and `QualityGateError`
- `load`: inspect SQL, exports, and `warehouse.duckdb`
- `cleanup`: inspect write permissions under `logs/etl/lineage/`

## Scenario 7: final outputs are missing

### Symptoms

- no files under `silver`, `rejected`, or `gold`
- the pipeline appears to have completed only partially

### Check

1. `quality_report` in lineage
2. lineage `artifacts`
3. active formats in `exports.mart_formats` and `exports.star_schema_formats`

### Action

1. Do not assume CSV exists by default: the current canonical output is `parquet`.
2. If CSV is required, enable it explicitly in `config/config.yaml`.
3. If even `parquet` is missing, inspect the upstream `load` failure.

## Recovery before rerun

Before re-running:

1. confirm the root cause is understood
2. fix config, input, or environment
3. decide whether you need:
   - a full execution
   - an offline execution
   - tests only
4. keep minimum evidence:
   - log
   - lineage
   - quality report

## Success criteria after recovery

Consider the incident resolved only when the relevant checks pass:

- the local command or DAG finishes green
- a fresh `logs/etl/lineage/lineage_<run_id>.json` is generated
- `gold` `parquet` outputs exist
- the `quality_report` contains no `critical_failures`
- `make test-integration` passes when the issue affected `load`/SQL/modeling
- Airflow scheduler and webserver are healthy again when orchestration was the issue

## Escalation

Escalate the incident when:

- the error persists after a reasonable local fix
- the external dependency (Open-Meteo) remains unavailable
- the quality gate repeatedly shows broken input or a broken contract
- SQL breaks analytical model invariants
- Airflow does not recover after `down/init/up`

## Quick references

- main README: [../README.md](../README.md)
- model SQL: [../sql/build_analytics_model.sql](../sql/build_analytics_model.sql)
