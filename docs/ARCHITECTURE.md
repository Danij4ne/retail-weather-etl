# Architecture

This document describes the current runtime architecture of `retail-weather-etl` as an ordered ETL flow.

For dataset schemas, guarantees, and compatibility rules, see [DATA_CONTRACT.md](./DATA_CONTRACT.md).
For why these runtime choices were made, see [DECISIONS.md](./DECISIONS.md).
For troubleshooting and recovery procedures, see [RUNBOOK.md](./RUNBOOK.md).

## 1. Pipeline and Support Phases

| Stage                | Main code                                                                                                                  | Purpose                                                                                                                       | Main output                                                         |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `Orchestration`            | `pipelines/pipeline.py`, `dags/etl_dag.py`                                                                             | Start and schedule the ETL flow in local and Airflow modes.                                                                   | Runtime execution context                                           |
| `Settings`                 | `config/config.yaml`, `etl/settings.py`                                                                                | Resolve paths, exports, weather settings, normalization maps, retries, and thresholds.                                        | Loaded runtime configuration                                        |
| `Extract`                  | `etl/extract.py`                                                                                                         | Read retail CSV inputs from `data/raw` and fetch daily weather data from Open-Meteo.                                        | Raw in-memory datasets and refreshed `data/raw/weather_daily.csv` |
| `Profiling`                | `etl/transform_parts/profiling.py`                                                                                       | Produce lightweight shape, null, duplicate, and early-violation signals before cleaning.                                      | Profiling logs and pre-clean quality signals                        |
| `Transform`                | `etl/transform_parts/transform_pipeline.py`, `etl/transform_parts/clean_*.py`, `etl/transform_parts/save_outputs.py` | Normalize values, parse fields, deduplicate rows, split valid vs rejected sales, and publish `silver`/`rejected` outputs. | `silver` Parquet datasets and `sales_rejected.csv`             |
| `Validations`              | `etl/validations.py`                                                                                                     | Enforce the post-clean contract and build the serialized quality report before the gold load proceeds.                        | Quality gate result +`quality_report`                             |
| `Load`                     | `etl/load.py`, `sql/build_analytics_model.sql`                                                                         | Build the DuckDB star schema and mart and publish `gold` outputs.                                                           | `gold` Parquet outputs + `warehouse.duckdb`                     |
| `Observability`            | logging setup,`etl/lineage.py`, `etl/alerts.py`                                                                        | Persist logs, lineage, the compact observability summary, and optional alerts.                                                | `pipeline.log`, lineage JSON, summary log, alerts                 |

## 2. Main Components

| Component                                     | Responsibility                                                                                                     |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `config/config.yaml`                        | Declares runtime paths, export formats, weather extraction parameters, normalization maps, and quality thresholds. |
| `pipelines/pipeline.py`                     | Runs the local end-to-end ETL flow, stage timing, logging, quality gate, lineage, and summary emission.            |
| `dags/etl_dag.py`                           | Executes the same logical flow under Airflow task orchestration.                                                   |
| `etl/settings.py`                           | Loads and validates project settings from `config/config.yaml` with runtime-safe defaults.                       |
| `etl/extract.py`                            | Builds raw input paths, retries transient API failures, fetches weather, and reads raw CSV files.                  |
| `etl/transform_parts/profiling.py`          | Summarizes nulls, duplicates, key duplication, and early rule violations before cleaning.                          |
| `etl/transform_parts/transform_pipeline.py` | Orchestrates the internal transform flow across cleaners, output publication, and optional post-clean checks.      |
| `etl/transform_parts/clean_customers.py`    | Normalizes customer fields and resolves duplicate customers by completeness.                                       |
| `etl/transform_parts/clean_products.py`     | Normalizes product names/categories, parses price, and resolves duplicate products.                                |
| `etl/transform_parts/clean_sales.py`        | Parses sales fields, resolves duplicate sales, and splits clean vs rejected rows with ordered `reject_reasons`.  |
| `etl/transform_parts/clean_weather.py`      | Normalizes weather records and resolves duplicate `(date, city)` entries.                                        |
| `etl/transform_parts/save_outputs.py`       | Projects published `silver` and `rejected` datasets to their public output schemas.                            |
| `etl/validations.py`                        | Enforces the post-clean silver contract and builds the serializable quality report.                                |
| `etl/load.py`                               | Executes the SQL model in DuckDB and exports `gold` artifacts.                                                   |
| `sql/build_analytics_model.sql`             | Defines the gold-layer star schema, fact table, and analytical mart built inside DuckDB.                           |
| `etl/lineage.py`                            | Persists run-level lineage and the compact observability summary.                                                  |
| `etl/alerts.py`                             | Sends optional runtime and Airflow failure alerts without masking the main pipeline error.                         |

## 3. Runtime Notes

- `Orchestration` is shown as a control phase, not as part of the data payload itself.
- Retail CSVs already live in `data/raw`, while `weather_daily.csv` is landed or refreshed during `Extract`.
- `Profiling` describes raw quality for logs and inspection; it does not change rows or publish cleaned datasets.
- `Transform` publishes `silver` and `rejected` outputs before `Validations`; the quality gate controls whether the pipeline may continue into the gold load.
- `Settings` centrally configure paths, normalization rules, quality thresholds, weather extraction, retries, and export formats.
- `Observability` is a side-effect phase: it collects evidence about runtime behavior rather than producing analytical datasets.
- Raw is landed source data, not a governed published dataset. The published layers are `silver`, `rejected` (sales audit), and `gold`. Canonical `silver` and `gold` artifacts are Parquet; `rejected` is CSV. Gold CSV export remains optional via config.
- `warehouse.duckdb` and local log files are runtime artifacts, not core code components.

## 4. Execution Environment

`pipelines/pipeline.py` and `dags/etl_dag.py` run the same logical ETL. They share `etl/` primitives — including `enforce_quality_report` and `assemble_lineage_report` — and differ in process boundaries, weather defaults, and when lineage is written.

`Docker` and Docker Compose provide a containerized runtime for Airflow. They are not the primary data-orchestration logic.

### Local

`pipeline.py` is a single-process run. Weather fetch defaults to on; lineage is written in `finally`.

### Airflow

`dags/etl_dag.py` splits the run into `extract`, `transform`, `load`, and `cleanup`. Weather fetch defaults to offline; `cleanup` always runs with `TriggerRule.ALL_DONE`.

## 5. Design Decisions

- `silver` is the public clean contract consumed by SQL
- `rejected` is published separately as an audit artifact
- `gold` is modeled in DuckDB through SQL rather than through in-memory analytical joins
- the canonical analytical outputs are Parquet
- clean sales remain in `fact_sales` even when weather or valid product price is missing
- `weather_sales_mart` intentionally excludes rows whose customer city remains unknown
- the current model is full refresh only; no incremental load or SCD behavior is implemented
