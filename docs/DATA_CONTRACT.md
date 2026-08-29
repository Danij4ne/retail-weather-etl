# Data Contract

This document defines the published dataset contracts for the current `retail-weather-etl` pipeline.

Current sources of truth:

- `silver` and `rejected` output schemas: `etl/transform_parts/save_outputs.py`
- cleaning and rejection semantics: `etl/transform_parts/clean_*.py`
- quality guarantees: `etl/validations.py`
- `gold` schema and derived metrics: `sql/build_analytics_model.sql`

Raw inputs are out of scope. They are treated as source data, not as governed published datasets.

## 1. Purpose and Scope

This contract defines:

- which datasets are published
- the grain and schema of each dataset
- the business rules that shape each published dataset
- the quality guarantees expected by downstream consumers
- which changes are backward-compatible vs breaking

It covers:

- `silver` datasets in `data/processed/silver/`
- `rejected` datasets in `data/processed/rejected/`
- `gold` star-schema and mart outputs in `data/processed/gold/`

It does not cover:

- logs
- Airflow operational metadata
- temporary staging files
- `warehouse.duckdb` as a multi-user warehouse product

## 2. Contract Conventions

- `silver`: cleaned, deduplicated domain datasets used as the public pre-SQL contract
- `rejected`: audited rows excluded from the clean business flow
- `gold`: analytical star schema and mart built from persisted `silver`
- `Logical type` describes business meaning
- `Storage type` describes published file representation for `silver` and `rejected`
- `DuckDB type` describes the current physical SQL type for `gold`
- `Nullable` is a contract-level expectation, not only a physical DB constraint
- `PK` means the dataset grain must be unique at that field or combination
- `FK` means the field references another published dataset logically, even when no DB FK is declared
- all datasets use `full refresh`
- canonical `silver` outputs are Parquet
- `rejected` outputs are published as CSV
- canonical `gold` outputs are Parquet; CSV exports are optional and configuration-dependent

## 3. Backward Compatibility Rules

- removing a published column is a breaking change
- renaming a published column is a breaking change
- changing a column meaning is a breaking change
- changing dataset grain is a breaking change
- tightening nullability is a breaking change
- changing the formula of a derived metric is a breaking semantic change
- changing the vocabulary or ordering of `reject_reasons` is a breaking change
- adding a new nullable column is backward-compatible
- adding a new non-nullable column requires an explicit contract decision

## 4. Dataset Inventory

| Layer.         | Dataset                     | Grain                                             | Canonical artifact                                                |
| ------------------- | --------------------------- | ------------------------------------------------- | ----------------------------------------------------------------- |
| `silver`          | `customers_clean.parquet`     | 1 row per `customer_id`                         | `data/processed/silver/customers_clean.parquet`                     |
| `silver`          | `products_clean.parquet`      | 1 row per `product_id`                          | `data/processed/silver/products_clean.parquet`                      |
| `silver`          | `sales_clean.parquet`         | 1 row per `sale_id`                             | `data/processed/silver/sales_clean.parquet`                         |
| `silver`          | `weather_daily_clean.parquet` | 1 row per (`date`, `city`)                    | `data/processed/silver/weather_daily_clean.parquet`                 |
| `rejected`        | `sales_rejected.csv`      | 1 rejected sales row after transform split        | `data/processed/rejected/sales_rejected.csv`                    |
| `gold`            | `dim_customers`           | 1 row per `customer_id`                         | `data/processed/gold/parquet/star_schema/dim_customers.parquet` |
| `gold`            | `dim_products`            | 1 row per `product_id`                          | `data/processed/gold/parquet/star_schema/dim_products.parquet`  |
| `gold`            | `dim_weather`             | 1 row per (`date`, `city`)                    | `data/processed/gold/parquet/star_schema/dim_weather.parquet`   |
| `gold`            | `fact_sales`              | 1 row per `sale_id`                             | `data/processed/gold/parquet/star_schema/fact_sales.parquet`    |
| `gold`            | `weather_sales_mart`      | 1 row per (`sale_date`, `city`, `category`) | `data/processed/gold/parquet/marts/weather_sales_mart.parquet`  |

## 5. Silver Contracts

Note: The structural `silver` contracts documented in this section are also validated at runtime via Pandera schemas in `etl/schemas/silver.py`, executed from `etl/validations.py` during `post_clean_checks`.

### 5.1 customers_clean.parquet

Contract summary:

- Grain: 1 row per `customer_id`
- Upstream: `data/raw/customers.csv`
- Downstream: `dim_customers`

| Column           | Logical type             | Storage type      | Nullable | Key role | Description                                                       |
| ---------------------- | ------------------------ | ----------------- | -------- | -------- | ----------------------------------------------------------------- |
| `customer_id`        | customer identifier      | Parquet integer field | No       | PK       | Canonical customer identifier after validation and deduplication. |
| `first_name`         | customer first name      | Parquet text field    | Yes      | none     | Trimmed and title-cased first name.                               |
| `last_name`          | customer last name       | Parquet text field    | Yes      | none     | Trimmed and title-cased last name.                                |
| `city`               | normalized customer city | Parquet text field    | Yes      | none     | Canonical city after `city_map` normalization.                  |
| `signup_date`        | customer signup date     | Parquet date field    | Yes      | none     | Parsed signup date; null when parsing fails.                      |

Contract rules:

- rows with invalid `customer_id` are dropped
- duplicate `customer_id` rows are resolved by completeness score; ties prefer the most recent parsed `signup_date`
- `customer_id` must be integer-like and unique after cleaning
- `signup_date` accepts year-month-day as `%Y-%m-%d` or `%Y/%m/%d`; any other layout becomes null

### 5.2 products_clean.parquet

Contract summary:

- Grain: 1 row per `product_id`
- Upstream: `data/raw/products.csv`
- Downstream: `dim_products`, `fact_sales`, `weather_sales_mart`

| Column               | Logical type               | Storage type           | Nullable | Key role | Description                                                         |
| ---------------------------- | -------------------------- | ---------------------- | -------- | -------- | ------------------------------------------------------------------- |
| `product_id`               | product identifier         | Parquet integer field      | No       | PK       | Canonical product identifier after validation and deduplication.    |
| `product_name`             | product display name       | Parquet text field         | Yes      | none     | Normalized display name.                                            |
| `category`                 | canonical product category | Parquet text field         | No       | none     | Canonical category after mapping and fallback to `Unknown`.       |
| `price`                    | product unit price         | Parquet numeric field      | Yes      | none     | Parsed positive price; null when missing, invalid, or non-positive. |
| `is_price_valid`           | price validity flag        | Parquet boolean-like field | No       | none     | `True` only when `price` is present and strictly positive.      |

Contract rules:

- rows with invalid `product_id` are dropped
- `category` is normalized through `category_map`; unmapped values become `Unknown`
- `price <= 0` does not reject the row; it becomes `null` and `is_price_valid = false`
- duplicate `product_id` rows are resolved by completeness score; ties prefer the highest valid parsed `price`

### 5.3 sales_clean.parquet

Contract summary:

- Grain: 1 row per `sale_id`
- Upstream: `data/raw/sales.csv`, `customers_clean.parquet`, `products_clean.parquet`
- Downstream: `fact_sales`, `weather_sales_mart`

| Column          | Logical type        | Storage type      | Nullable | Key role.                            | Description                                                |
| -------------------- | ------------------- | ----------------- | -------- | ------------------------------------ | ---------------------------------------------------------- |
| `sale_id`          | sale identifier     | Parquet integer field | No       | PK                                   | Canonical sale identifier after parsing and deduplication. |
| `customer_id`      | customer identifier | Parquet integer field | No       | FK ->`customers_clean.customer_id` | Customer key carried into analytics.                       |
| `product_id`       | product identifier  | Parquet integer field | No       | FK ->`products_clean.product_id`   | Product key carried into analytics.                        |
| `sale_date`        | sale date           | Parquet date field    | No       | none                                 | Parsed sale date.                                          |
| `quantity`         | sold units          | Parquet integer field | No       | none                                 | Clean positive integer quantity.                           |
| `discount`         | discount percentage | Parquet numeric field | No       | none                                 | Discount percentage in the `[0, 100]` range.             |

Contract rules:

- parsing happens before rejection for IDs, `sale_date`, `quantity`, and `discount`
- `sale_date` accepts year-month-day as `%Y-%m-%d` or `%Y/%m/%d`; any other layout is `invalid_sale_date`
- missing discount is normalized to `0`
- `quantity` must be integer-like and strictly greater than `0`
- customer and product keys must resolve against cleaned dimensions
- deduplication only applies to rows with valid `sale_id`; ties prefer the most recent parsed `sale_date`

### 5.4 weather_daily_clean.parquet

Contract summary:

- Grain: 1 row per (`date`, `city`)
- Upstream: `data/raw/weather_daily.csv`
- Downstream: `dim_weather`, `fact_sales`, `weather_sales_mart`

| Column.           | Logical type              | Storage type      | Nullable | Key role     | Description                               |
| ----------------------- | ------------------------- | ----------------- | -------- | ------------ | ----------------------------------------- |
| `date`                | weather observation date  | Parquet date field    | No       | PK component | Daily weather date.                       |
| `city`                | normalized city name      | Parquet text field    | No       | PK component | Canonical city used for weather matching. |
| `temp_c`              | mean daily temperature    | Parquet numeric field | Yes      | none         | Mean daily temperature in Celsius.        |
| `precip_mm`           | daily precipitation       | Parquet numeric field | Yes      | none         | Precipitation in millimeters.             |
| `precip_hours`        | daily precipitation hours | Parquet numeric field | Yes      | none         | Precipitation duration.                   |
| `weather_code`        | weather condition code    | Parquet integer field | Yes      | none         | Open-Meteo daily weather code.            |

Contract rules:

- `date` is parsed from the raw payload as year-month-day (`%Y-%m-%d` or `%Y/%m/%d`); any other layout becomes null
- `city` is normalized to a canonical title-cased form
- negative `precip_mm` values are coerced to `null`, not rejected
- duplicate (`date`, `city`) rows are resolved by keeping the most complete row

## 6. Rejected Contract

### 6.1 sales_rejected.csv

Contract summary:

- Grain: 1 rejected sales row after parsing, scoring, deduplication, and reject split
- Upstream: `data/raw/sales.csv`, `customers_clean.parquet`, `products_clean.parquet`
- Downstream: audit and troubleshooting only

| Column               | Logical type                          | Storage type    | Nullable | Key role | Description                                                 |
| ---------------------------- | ------------------------------------- | --------------- | -------- | -------- | ----------------------------------------------------------- |
| `sale_id `                 | source-preserving sale identifier     | CSV audit field | Yes      | none     | Audit projection of the rejected sale identifier value.     |
| `customer_id`              | source-preserving customer identifier | CSV audit field | Yes      | none     | Audit projection of the rejected customer identifier value. |
| `product_id`               | source-preserving product identifier  | CSV audit field | Yes      | none     | Audit projection of the rejected product identifier value.  |
| `sale_date`                | source-preserving sale date           | CSV audit field | Yes      | none     | Audit projection of the rejected sale date value.           |
| `quantity`                 | source-preserving quantity payload    | CSV audit field | Yes      | none     | Audit projection of the rejected quantity value.            |
| `discount`                 | source-preserving discount payload    | CSV audit field | Yes      | none     | Audit projection of the rejected discount value.            |
| `reject_reasons`           | ordered rejection reason list         | CSV text field  | No       | none     | Stable pipe-separated list of rejection reasons.            |

Contract rules:

- this dataset is an audit artifact, not an analytical input
- it intentionally preserves rejected payload more closely than `sales_clean.parquet`
- only the public 7-column projection is published; internal helper columns are not
- current `reject_reasons` vocabulary and order are: `missing_sale_id`, `invalid_sale_id`, `missing_customer_id`, `invalid_customer_id`, `missing_product_id`, `invalid_product_id`, `invalid_sale_date`, `unknown_customer_id`, `unknown_product_id`, `invalid_quantity`, `invalid_discount`, `discount_out_of_range`

## 7. Gold Contracts

Global publication rule:

- canonical `gold` artifacts are Parquet
- CSV exports are optional and configuration-dependent

### 7.1 dim_customers

Contract summary:

- Grain: 1 row per `customer_id`
- Upstream: `customers_clean.parquet` via `stg_customers`
- Canonical artifact: `data/processed/gold/parquet/star_schema/dim_customers.parquet`

| Column          | DuckDB type | Nullable | Key role | Description                                                      |
| --------------- | ----------- | -------- | -------- | ---------------------------------------------------------------- |
| `customer_id` | `INTEGER` | No       | PK       | Customer identifier carried from silver.                         |
| `first_name`  | `VARCHAR` | Yes      | none     | Customer first name.                                             |
| `last_name`   | `VARCHAR` | Yes      | none     | Customer last name.                                              |
| `city`        | `VARCHAR` | Yes      | none     | Canonical city used later for weather joining and mart grouping. |
| `signup_date` | `DATE`    | Yes      | none     | Customer signup date cast to SQL date.                           |

Contract rules:

- built as a distinct projection from `stg_customers`
- preserves the silver business grain

### 7.2 dim_products

Contract summary:

- Grain: 1 row per `product_id`
- Upstream: `products_clean.parquet` via `stg_products`
- Canonical artifact: `data/processed/gold/parquet/star_schema/dim_products.parquet`

| Column             | DuckDB type | Nullable | Key role | Description                                             |
| ------------------ | ----------- | -------- | -------- | ------------------------------------------------------- |
| `product_id`     | `INTEGER` | No       | PK       | Product identifier carried from silver.                 |
| `product_name`   | `VARCHAR` | Yes      | none     | Product name.                                           |
| `category`       | `VARCHAR` | No       | none     | Canonical category label.                               |
| `price`          | `DOUBLE`  | Yes      | none     | Product unit price.                                     |
| `is_price_valid` | `INTEGER` | No       | none     | SQL-normalized validity flag encoded as `1` or `0`. |

Contract rules:

- built as a distinct projection from `stg_products`
- invalid prices remain represented as `price = null` plus `is_price_valid = 0`

### 7.3 dim_weather

Contract summary:

- Grain: 1 row per (`date`, `city`)
- Upstream: `weather_daily_clean.parquet` via `stg_weather`
- Canonical artifact: `data/processed/gold/parquet/star_schema/dim_weather.parquet`

| Column           | DuckDB type | Nullable | Key role               | Description                                               |
| ---------------------- | ----------- | -------- | ---------------------- | --------------------------------------------------------- |
| `weather_sk`         | `VARCHAR` | No       | PK                     | Deterministic surrogate weather key generated from `date|city` with `md5`. |
| `date`               | `DATE`    | No       | business key component | Weather date.                                             |
| `city`               | `VARCHAR` | No       | business key component | City used for weather matching.                           |
| `temp_c`             | `DOUBLE`  | Yes      | none                   | Mean daily temperature.                                   |
| `precip_mm`          | `DOUBLE`  | Yes      | none                   | Daily precipitation in millimeters.                       |
| `precip_hours`       | `DOUBLE`  | Yes      | none                   | Daily precipitation duration.                             |
| `weather_code`       | `INTEGER` | Yes      | none                   | Open-Meteo weather code.                                  |
| `rain_bucket`        | `VARCHAR` | No       | none                   | Derived rain bucket.                                      |
| `temp_bucket`        | `VARCHAR` | No       | none                   | Derived temperature bucket.                               |

Contract rules:

- `weather_sk` is generated with `md5(CAST(date AS VARCHAR) || '|' || city)`
- `rain_bucket` and `temp_bucket` are derived from configuration-driven thresholds
- a null `precip_mm` or `temp_c` yields the literal `unknown`; it must not inherit the ELSE climate class
- one row per (`date`, `city`) is a required precondition for downstream joins and mart logic; a broken grain fails the load
- a null `weather_sk` fails the load; uniqueness checks are not a substitute because `COUNT(DISTINCT)` ignores nulls

### 7.4 fact_sales

Contract summary:

- Grain: 1 row per `sale_id`
- Upstream: `sales_clean.parquet`, `dim_customers`, `dim_products`, `dim_weather`
- Downstream: `weather_sales_mart`
- Canonical artifact: `data/processed/gold/parquet/star_schema/fact_sales.parquet`

| Column                        | DuckDB type       | Nullable | Key role                           | Description                                                 |
| ----------------------------------------- | ----------------- | -------- | ---------------------------------- | ----------------------------------------------------------- |
| `sale_id`                               | `INTEGER`       | No       | PK                                 | Clean sale identifier.                                      |
| `customer_id`                           | `INTEGER`       | No       | FK ->`dim_customers.customer_id` | Customer dimension key.                                     |
| `product_id`                            | `INTEGER`       | No       | FK ->`dim_products.product_id`   | Product dimension key.                                      |
| `weather_sk`                            | `VARCHAR`       | Yes      | FK ->`dim_weather.weather_sk`    | Weather surrogate key when a date-city match exists.        |
| `sale_date`                             | `DATE`          | No       | degenerate dimension               | Transaction date.                                           |
| `quantity`                              | `INTEGER`       | No       | none                               | Sold units.                                                 |
| `discount`                              | `DOUBLE`        | No       | none                               | Discount percentage.                                        |
| `unit_price_at_sale`                    | `DOUBLE`        | Yes      | none                               | Catalog unit price of the product at this full refresh, not a guaranteed sale-time price. |
| `has_weather_match`                     | `INTEGER`       | No       | none                               | `1` when weather matched, else `0`.                     |
| `has_valid_price`                       | `INTEGER`       | No       | none                               | `1` when a valid product price exists, else `0`.        |
| `gross_amount`                          | `DECIMAL(18,2)` | Yes      | none                               | `quantity * unit_price_at_sale` when price is valid.      |
| `discount_amount`                       | `DECIMAL(18,2)` | Yes      | none                               | Discount amount derived from gross amount and `discount`. |
| `net_amount`                            | `DECIMAL(18,2)` | Yes      | none                               | `gross_amount - discount_amount` when price is valid.     |

Contract rules:

- customer and product joins are inner joins; weather join is left join on (`sale_date`, `customer.city`)
- weather gaps do not remove sales rows
- invalid product prices do not remove sales rows
- `unit_price_at_sale` is derived during the full-refresh load from the current cleaned product price for the matching `product_id`; it is an analytical catalog-price proxy, not a guaranteed historical sale-time price
- amount columns are populated only when `has_valid_price = 1`

### 7.5 weather_sales_mart

Contract summary:

- Grain: 1 row per (`sale_date`, `city`, `category`)
- Upstream: `fact_sales`, `dim_customers`, `dim_products`, `dim_weather`
- Canonical artifact: `data/processed/gold/parquet/marts/weather_sales_mart.parquet`

| Column.                    | DuckDB type       | Nullable | Key role        | Description                                                            |
| ------------------------------------ | ----------------- | -------- | --------------- | ---------------------------------------------------------------------- |
| `sale_date`                        | `DATE`          | No       | grain component | Aggregation date.                                                      |
| `city`                             | `VARCHAR`       | No       | grain component | Aggregation city.                                                      |
| `category`                         | `VARCHAR`       | No       | grain component | Aggregation category.                                                  |
| `total_units`                      | `HUGEINT`       | No       | none            | Sum of sold units.                                                     |
| `num_orders`                       | `BIGINT`        | No       | none            | Number of fact rows in the group.                                      |
| `num_orders_priced`                | `HUGEINT`       | No       | none            | Count of rows with `has_valid_price = 1`.                            |
| `total_revenue`                    | `DECIMAL(38,2)` | Yes      | none            | Sum of `net_amount` over priced rows only, at current catalog price; not historical billed revenue. |
| `avg_ticket`                       | `DOUBLE`        | Yes      | none            | `total_revenue / num_orders_priced` when priced orders exist.        |
| `avg_temp_c`                       | `DOUBLE`        | Yes      | none            | Average temperature across matched weather rows.                       |
| `avg_precip_mm`                    | `DOUBLE`        | Yes      | none            | Average precipitation in millimeters.                                  |
| `avg_precip_hours`                 | `DOUBLE`        | Yes      | none            | Average precipitation duration.                                        |
| `valid_price_rate`                 | `DOUBLE`        | No       | none            | Percentage of priced orders over total orders, in the `0-100` range. |
| `rain_bucket`                      | `VARCHAR`       | Yes      | none            | Weather bucket carried from the unique date-city weather row.          |
| `temp_bucket`                      | `VARCHAR`       | Yes      | none            | Temperature bucket carried from the unique date-city weather row.      |

Contract rules:

- grouped by `sale_date`, `city`, and `category`
- excludes rows where `customer.city` is null
- `total_revenue` uses priced rows only and inherits Decision 16: it is estimated at the catalog price of this refresh, not historical billed revenue
- `avg_ticket` divides by `num_orders_priced`, not by total orders
- `valid_price_rate` is a percentage, not a `0-1` ratio
- a null bucket means no weather row joined; `unknown` means a weather row existed but that metric was null

## 8. Cross-Dataset Rules

- `silver` is the public clean contract consumed by the SQL model
- `rejected` is the public audit contract for excluded sales rows
- `gold` is built only from persisted `silver` outputs
- referential integrity for sales against customers and products is enforced before SQL load
- weather coverage and price coverage are modeled as flags and metrics rather than by dropping otherwise valid sales
- the mart is stricter than the fact table: it requires known customer city to support city-level aggregation

## 9. Validation Enforcement

| Category                 | Current enforcement                                                                                                                                                                                                                                             | Main source                                                                                        |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Blocking enforcement     | Missing required `silver` columns, incompatible `silver` types, duplicate business keys after cleaning, orphan customer/product keys in clean sales, invalid `price` / `is_price_valid` consistency, invalid weather uniqueness at (`date`, `city`), and null weather `date` / `city`. Gold also fails the load when SQL invariants break: unique `customer_id` / `product_id` / `weather_sk` / `(date, city)` / `sale_id`, non-null `dim_weather.weather_sk`, no fact orphans, and `num_orders_priced <= num_orders`. | `etl/transform_parts/save_outputs.py`, `etl/validations.py`, `sql/build_analytics_model.sql` |
| Warning-only enforcement | Reject rate, unknown-city rate, or valid-price coverage beyond configured `quality.*` floors; weather coverage gaps; residual product-name digit anomalies                                                                                                          | `etl/validations.py`                                                                             |
| Regression enforcement   | Output column contracts, cleaning rules, reject splitting, quality-gate behavior, SQL invariants, metric consistency checks, export-format behavior                                                                                                             | `tests/unit/*`, `tests/integration/*`                                                          |
| Lineage evidence         | Stage timings, row counts (`extract`, `cleaned`, `rejected`, `unpublished` = `extract − cleaned − rejected`: invalid customer/product keys, losing duplicates including weather and sales, and any other row that reaches neither silver nor `sales_rejected`; `mart` = published `weather_sales_mart` size, `mart_preview` = capped sample of at most 10 rows), serialized `quality_report`, artifact fingerprints, final run status and errors                                                                                                                                                    | `logs/etl/lineage/lineage_<run_id>.json`, `[OBSERVABILITY] summary=...`                        |

## 10. Source of Truth and Maintenance

Review this document whenever any of the following change:

- `etl/transform_parts/save_outputs.py`
- `etl/transform_parts/clean_*.py`
- `etl/transform_parts/date_parsing.py`
- `etl/validations.py`
- `sql/build_analytics_model.sql`
- export-format behavior in `etl/load.py`
- lineage row-count semantics in `etl/lineage.py`
