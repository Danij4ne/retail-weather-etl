# Testing Guide

This document explains how testing is organized in `retail-weather-etl`, what each test layer covers, and which command to use in common workflows.

## Purpose

The test suite is designed to protect three things:

- dataset cleaning and normalization rules
- contract and quality-gate behavior
- DuckDB / SQL / filesystem integration flows

The project uses `pytest` and splits tests into `unit` and `integration`.

## Quick Commands

Run the full suite:

```bash
make test
```

Run everything except integration:

```bash
make test-fast
```

Run unit tests only:

```bash
make test-unit
```

Run integration tests only:

```bash
make test-integration
```

Direct `pytest` marker usage:

```bash
uv run python -m pytest -q -m integration
uv run python -m pytest -q -m "not integration"
```

## Test Layout

```text
tests/
├── conftest.py
├── factories/
│   └── sample_data.py
├── unit/
└── integration/
```

### `tests/unit/`

Unit tests validate isolated behavior with small in-memory datasets and focused expectations.

Main coverage:

- `test_clean_customers.py`: customer normalization, date parsing, and deduplication rules
- `test_clean_products.py`: product normalization, category mapping, price parsing, and validity flags
- `test_clean_sales.py`: sales parsing, reject splitting, duplicate handling, and `reject_reasons`
- `test_clean_weather.py`: weather normalization, numeric coercion, and uniqueness by (`date`, `city`)
- `test_date_parsing.py`: explicit year-month-day date formats (`%Y-%m-%d`, `%Y/%m/%d`) and batch-independent rejection of other layouts
- `test_profiling.py`: profiling summaries and early quality-signal behavior
- `test_validations.py`: blocking and warning-only quality-gate checks, including Pandera-backed `silver` contract failures
- `test_transform.py`: transform orchestration and output structure
- `test_extract.py`: CSV source loading and weather extraction helpers
- `test_load.py`: load-layer export behavior and DuckDB-related logic
- `test_settings.py`: config loading, defaults, and validation behavior
- `test_lineage.py`: lineage payloads and observability summary structure
- `test_alerts.py`: alert formatting and optional alert delivery behavior
- `test_pipeline.py`: local pipeline orchestration flow and stage behavior
- `test_dag_parse.py`: Airflow DAG import/parse safety and `RETAIL_ETL_FETCH_WEATHER` flag handling

Use unit tests when you change:

- cleaning logic
- validation rules
- settings resolution
- pipeline orchestration behavior
- lineage or alerts behavior

### `tests/integration/`

Integration tests validate end-to-end interactions across filesystem, persisted outputs, DuckDB, and SQL.

Main coverage:

- `test_save_silver_and_rejected.py`: persisted `silver` Parquet datasets plus `sales_rejected.csv` schema/files
- `test_load_build_analytics_model.py`: DuckDB star schema, deterministic `dim_weather.weather_sk`, fact table, mart logic, and SQL-derived metrics
- `test_run_pipeline_offline_smoke.py`: offline end-to-end pipeline smoke run using existing weather data

Use integration tests when you change:

- output schemas or export behavior
- DuckDB load logic
- `build_analytics_model.sql`
- runtime artifact generation
- end-to-end pipeline flow

## Fixtures and Factories

### `tests/conftest.py`

Shared fixtures build reusable inputs and transformed outputs for the suite.

Examples:

- raw customer / product / sales / weather fixtures
- cleaned dimension fixtures
- full transform outputs with `run_checks=False` and `save_outputs=False`
- healthy cleaned/rejected data for validation tests

### `tests/factories/sample_data.py`

Factories generate small dirty datasets that intentionally trigger business rules.

They are used to test things like:

- deduplication
- parsing failures
- referential integrity failures
- reject splitting
- normalization maps
- weather cleaning edge cases

## Marker Conventions

`pytest.ini` defines:

- `integration`: tests that validate filesystem / DuckDB / SQL integration flows

That means:

- tests without this marker are treated as the fast default layer
- integration tests can be excluded for quick local feedback

## What the Suite Enforces

At a high level, the suite protects:

- published `silver` and `rejected` schemas
- Pandera-backed `silver` contract enforcement
- `reject_reasons` behavior
- quality-gate pass/fail behavior
- SQL model invariants
- deterministic `dim_weather.weather_sk`
- star-schema and mart metrics
- export format behavior
- pipeline offline execution smoke path

## Suggested Workflow

When changing cleaning or validation logic:

```bash
make test-unit
```

When changing SQL, DuckDB load, or published outputs:

```bash
make test-integration
```

Before finishing a larger change:

```bash
make test
```

## Notes

- Run commands from the project root: `retail-weather-etl/`
- Integration tests are the most representative, but also the slowest
- Unit tests are the fastest way to validate business-rule changes locally
