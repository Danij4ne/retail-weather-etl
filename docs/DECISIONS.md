# Technical Decisions

## Purpose

This document records the main technical and data-cleaning decisions that shape the current ETL behavior. It is not an ADR log for every small change; it is a concise explanation of the deliberate choices that affect architecture, runtime behavior, and published data.

## How to Read This Document

Each decision is written in the same format:

- `Context`: what problem or constraint exists
- `Decision`: what the project does
- `Why`: why that choice was made
- `Consequences`: what that choice implies in practice

## Platform and Pipeline Decisions

### Decision 1. Local-first analytics with DuckDB

- `Context`: the project needs a lightweight analytical engine that can be run locally without external infrastructure.
- `Decision`: use DuckDB as the modeling and persistence engine for the star schema and mart.
- `Why`: DuckDB fits the project scope well, keeps setup small, works naturally with Parquet, and is easy to inspect during development.
- `Consequences`: the project stays simple to run and is intentionally optimized for local analytics rather than distributed compute. Canonical silver and gold artifacts are Parquet. The loader can still emit CSV when configured, but the runtime contract assumes Parquet.

### Decision 2. Full-refresh pipeline instead of incremental loads

- `Context`: the source data volume is small enough to rebuild consistently from scratch.
- `Decision`: run the ETL as a full refresh on each execution.
- `Why`: full refresh reduces orchestration complexity, avoids incremental-state bugs, and makes the outputs easier to reason about and test.
- `Consequences`: each run rebuilds silver and gold from the current raw inputs, and the design does not currently optimize for very large historical loads.

### Decision 3. Run the quality gate after transform and before load

- `Context`: the pipeline needs to validate cleaned outputs before building analytical gold models.
- `Decision`: run the quality gate after transformation, not during raw ingestion, and block the gold load only when critical failures exist.
- `Why`: this keeps cleaning and validation as separate concerns and allows the pipeline to produce inspectable silver and rejected outputs even when gold should not be built.
- `Consequences`: silver and rejected outputs may exist for failed runs, while the gold load is prevented when blocking issues are detected. Gold still asserts its own grain at load time through SQL invariants in `build_analytics_model.sql`, because `build_analytics_model` and a hand-run `.sql` can skip this gate.

### Decision 4. Treat lineage and observability as first-class runtime outputs

- `Context`: ETL runs need traceability beyond application logs.
- `Decision`: persist lineage JSON, log stage timings, and emit optional alerts as runtime outputs.
- `Why`: observability should stay out of the business data contract.
- `Consequences`: lineage and summaries must stay aligned with code changes.

## Data Cleaning Decisions

### Decision 5. Make normalization rules configuration-driven

- `Context`: city/category maps and product-name cleanup may change without a code change.
- `Decision`: resolve maps and flags from settings, with fallbacks to defaults.
- `Why`: keep those rules in one place instead of hardcoding them in each cleaner.
- `Consequences`: missing YAML keys fall back to known defaults rather than failing the run.

### Decision 6. Drop invalid primary keys in customers and products

- `Context`: customer and product rows cannot be trusted downstream when their business key is missing or malformed.
- `Decision`: drop `customers` and `products` rows whose IDs are missing, blank, non-numeric, or non-integer.
- `Why`: the pipeline should not invent or guess entity identities.
- `Consequences`: some raw rows are discarded early, but the published dimensions remain structurally trustworthy. Invalid keys are not written to a `customers_rejected` or `products_rejected` file; losing duplicates (Decision 7) are unpublished the same way. The cleaner logs a warning with a **count**, not the dropped payload. Rows with a matching sale can still surface through sales `unknown_*` / `invalid_*` reject reasons; an invalid customer or product with no sales does not reappear in `sales_rejected`. Those drops are part of lineage `unpublished` (`extract − cleaned − rejected`), not a published audit file.

### Decision 7. Deduplicate using deterministic scoring instead of first-row wins

- `Context`: duplicate business keys exist in multiple raw datasets, and not all duplicate candidates are equally complete.
- `Decision`: compute a completeness score per row and keep the best candidate using stable sorting.
- `Why`: deterministic score-based deduplication is more defensible than arbitrary `keep="first"` behavior.
- `Consequences`: duplicate resolution is reproducible and biased toward the most complete record. What is auditable is the **rule** (completeness score, stable `mergesort`, `keep="first"`). Losing rows are not published; they are counted in lineage `unpublished` together with Decision 6 drops. Reconstructing a loser still requires the raw file.

### Decision 8. Split sales into clean and rejected outputs

- `Context`: sales contains the highest concentration of business-rule failures, malformed IDs, invalid dates, and orphan foreign keys.
- `Decision`: clean sales into `sales_clean` and publish rejected rows separately as `sales_rejected`.
- `Why`: silently dropping invalid sales would hide data quality problems and remove auditability.
- `Consequences`: consumers get a clean analytical sales table, while operators still retain the rejected payload for inspection and root-cause analysis. Dimensions do not get a parallel rejected file; see Decision 6.

### Decision 9. Make reject reasons explicit, ordered, and auditable

- `Context`: rejected sales rows need a stable explanation that downstream users and tests can trust.
- `Decision`: build `reject_reasons` from an ordered, explicit vocabulary such as `missing_*`, `invalid_*`, `unknown_*`, and business-rule violations.
- `Why`: stable reject semantics are easier to analyze than free-form or non-deterministic error messages.
- `Consequences`: rejected outputs become comparable across runs, and changes to reject vocabulary should be treated as a contract change.

### Decision 10. Preserve products with invalid price and expose `is_price_valid`

- `Context`: a product can still be a valid entity even when its price is missing or unusable.
- `Decision`: keep the product row, set `price` to null when invalid, and publish `is_price_valid` as an explicit flag.
- `Why`: entity identity and price quality are different concerns.
- `Consequences`: product coverage remains higher, while downstream logic can explicitly reason about valid versus invalid price availability.

### Decision 11. Treat missing discount as zero, but malformed discount as invalid

- `Context`: missing discount values in sales often represent no discount rather than data corruption.
- `Decision`: coerce missing discount to `0`, but treat non-parsable discount values as invalid and reject the row.
- `Why`: this is a pragmatic business default that still preserves strictness for malformed data.
- `Consequences`: the pipeline is tolerant of blank discounts but not of ambiguous or corrupted discount fields.

### Decision 12. Repair weather anomalies conservatively

- `Context`: weather data can contain anomalies such as negative precipitation or duplicated `(date, city)` rows.
- `Decision`: coerce negative precipitation to null and deduplicate weather rows by completeness score per `(date, city)`.
- `Why`: conservative repair preserves useful weather records without pretending obviously invalid precipitation values are trustworthy.
- `Consequences`: weather coverage remains high, while impossible precipitation values do not leak into analytics as valid facts. Derived buckets classify a missing measurement as `unknown` instead of inventing a climate class.

### Decision 13. Separate blocking failures from warning-only quality issues

- `Context`: not every quality issue should stop the gold load. The synthetic source is intentionally dirty, so textbook thresholds (10% reject, 95% price coverage) fire on every healthy run.
- `Decision`: treat contract breaks, orphan foreign keys, null/duplicate keys, and similar structural issues as blocking failures, while reject-rate and coverage issues remain warnings. Warning floors live in `quality.*` and are calibrated to the observed baseline of this source, not to an ideal rate. A warning fires only when a floor is breached.
- `Why`: this keeps the quality gate strict where correctness matters and pragmatic where monitoring is more appropriate than hard failure. Alerts that always fire are not alerts.
- `Consequences`: the pipeline can complete with warnings, but structural trust violations still stop analytical publication. `quality.*` values are **regression floors** against the current synthetic baseline, not absolute quality SLOs: a healthy run of this fixture should emit no warnings. Recalibrate `quality.max_reject_rate`, `quality.min_valid_price_coverage`, and `quality.max_unknown_city_rate` if the fixture dirtiness changes. Metrics still appear in `quality_report` even when no warning is emitted.

### Decision 14. Use a deterministic surrogate key for `dim_weather`

- `Context`: `dim_weather` is naturally identified by (`date`, `city`), but downstream modeling benefits from a single-column key.
- `Decision`: generate `weather_sk` as a deterministic surrogate key using `md5(CAST(date AS VARCHAR) || '|' || city)` instead of positional numbering.
- `Why`: a deterministic key stays stable for the same business entity across full-refresh runs and does not depend on row ordering.
- `Consequences`: `weather_sk` becomes a reproducible text key, joins remain simple, and the model is better prepared for future cross-run comparisons or incremental evolution.

### Decision 15. Express structural silver contracts as executable Pandera schemas

- `Context`: silver contracts need runtime checks that stay next to the documented schema.
- `Decision`: encode structural silver contracts as Pandera schemas in `etl/schemas/silver.py` and run them from `post_clean_checks`.
- `Why`: one schema definition instead of duplicated column and type asserts.
- `Consequences`: Pandera is part of the validation stack. Business-quality and relational checks stay in `etl/validations.py`.

### Decision 16. Use current product price as the analytical sale price in the full-refresh model

- `Context`: the current sales source does not publish a guaranteed historical unit price per sale, while the analytical model still needs a price basis to estimate revenue-style metrics.
- `Decision`: derive `fact_sales.unit_price_at_sale` from the current cleaned product price joined by `product_id` during the full-refresh load.
- `Why`: this keeps the model simple and allows revenue-style metrics without introducing SCD2 complexity or a separate historical price snapshot flow that the current sources do not support.
- `Consequences`: `unit_price_at_sale`, `gross_amount`, `discount_amount`, `net_amount`, and mart `total_revenue` reflect the product price available at refresh time, not a guaranteed historical sale-time price. The published column names are analytical proxies; SQL comments must not describe the price as frozen. Historical revenue analytics would require either a source-level sale price snapshot or a historized product-price model.

### Decision 17. Parse dates with explicit year-month-day formats

- `Context`: `pd.to_datetime` without `format` infers one layout from the whole batch. The same string can be accepted, rejected, or silently misread depending on neighboring rows and the pandas version. Tests that expected `UserWarning` froze that heuristic as if it were a business rule.
- `Decision`: parse `sale_date`, `signup_date`, and weather `date` with a shared cascade: `%Y-%m-%d` first, then `%Y/%m/%d` on the remainder. Unrecognized values become NaT. The formats are a contract constant, not YAML.
- `Why`: the raw files are year-month-day with two separators. Explicit formats make each row's destination independent of the rest of the lot and avoid day-first ambiguity such as `01/02/2025`.
- `Consequences`: `2025/01/16` stays valid even in a hyphen-only or slash-only batch. Day-first strings such as `31/01/2025` are invalid: sales go to `sales_rejected` with `invalid_sale_date`; customer `signup_date` becomes null.

## Orchestration and Runtime Decisions

### Decision 18. Serialize DAG runs instead of allowing concurrency

- `Context`: silver, gold, and `warehouse.duckdb` live at fixed paths and are not partitioned per run.
- `Decision`: set `max_active_runs=1` on the DAG.
- `Why`: overlapping runs would race on the same artifacts, and DuckDB allows a single writer per file.
- `Consequences`: Airflow runs queue instead of overlapping, which is acceptable for a full-refresh pipeline. `max_active_runs=1` does not serialize `pipelines/pipeline.py`; two local runs (or local plus DAG against the same paths) can still race on `warehouse.duckdb` and fixed silver/gold artifacts.

## Change Rules

- Update this document when a decision changes the runtime behavior, the published data semantics, or the operational model of the ETL.
- Do not use this file for minor implementation refactors that do not change behavior.
- Image, Compose bind, and local threat-model details belong in `RUNBOOK.md`.
- If a decision materially changes published schemas or rejection semantics, update `DATA_CONTRACT.md` as well.
