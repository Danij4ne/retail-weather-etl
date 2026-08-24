-- =========================
-- 1) STAGING (clean silver Parquet)
-- =========================
-- Current materialization strategy: FULL REFRESH per execution.
-- Each run recreates tables with CREATE OR REPLACE to prioritize simplicity
-- and reproducibility at this data volume (no incremental/SCD for now).

CREATE OR REPLACE TABLE stg_customers AS
SELECT
  customer_id::INTEGER AS customer_id,
  first_name,
  last_name,
  city,
  signup_date::DATE AS signup_date
FROM read_parquet(getvariable('silver_customers_path'));

CREATE OR REPLACE TABLE stg_products AS
SELECT
  product_id::INTEGER AS product_id,
  product_name,
  category,
  price::DOUBLE AS price,
  CASE
    WHEN lower(CAST(is_price_valid AS VARCHAR)) IN ('1', 'true', 't', 'yes', 'y') THEN 1
    ELSE 0
  END AS is_price_valid
FROM read_parquet(getvariable('silver_products_path'));

CREATE OR REPLACE TABLE stg_sales AS
SELECT
  sale_id::INTEGER AS sale_id,
  customer_id::INTEGER AS customer_id,
  product_id::INTEGER AS product_id,
  sale_date::DATE AS sale_date,
  quantity::INTEGER AS quantity,
  discount::DOUBLE AS discount
FROM read_parquet(getvariable('silver_sales_path'));

CREATE OR REPLACE TABLE stg_weather AS
SELECT
  date::DATE AS date,
  city,
  temp_c::DOUBLE AS temp_c,
  precip_mm::DOUBLE AS precip_mm,
  precip_hours::DOUBLE AS precip_hours,
  weather_code::INTEGER AS weather_code
FROM read_parquet(getvariable('silver_weather_path'));


-- =========================
-- 2) DIMENSIONS (STAR)
-- =========================
-- Silver is already 1 row per business key. SQL projects that grain;
-- it does not re-deduplicate. SELECT DISTINCT would only hide duplicate
-- keys that differ on attributes and would fan out the fact join.

CREATE OR REPLACE TABLE dim_customers AS
SELECT
  customer_id,
  first_name,
  last_name,
  city,
  signup_date
FROM stg_customers
ORDER BY customer_id ASC;


CREATE OR REPLACE TABLE dim_products AS
SELECT
  product_id,
  product_name,
  category,
  price,
  is_price_valid
FROM stg_products
ORDER BY product_id ASC;


CREATE OR REPLACE TABLE dim_weather AS
SELECT
  md5(CAST(date AS VARCHAR) || '|' || city) AS weather_sk,
  date,
  city,
  temp_c,
  precip_mm,
  precip_hours,
  weather_code,
  -- A null measurement must not fall through to ELSE (that invents heavy_rain / hot).
  CASE
    WHEN precip_mm IS NULL THEN 'unknown'
    WHEN precip_mm = 0 THEN 'no_rain'
    WHEN precip_mm < CAST(getvariable('rain_light_rain_lt_mm') AS DOUBLE) THEN 'light_rain'
    ELSE 'heavy_rain'
  END AS rain_bucket,
  CASE
    WHEN temp_c IS NULL THEN 'unknown'
    WHEN temp_c < CAST(getvariable('temp_cold_lt_c') AS DOUBLE) THEN 'cold'
    WHEN temp_c <= CAST(getvariable('temp_mild_lte_c') AS DOUBLE) THEN 'mild'
    ELSE 'hot'
  END AS temp_bucket
FROM stg_weather;


-- =========================
-- 3) FACT (business keys + weather surrogate)
-- =========================

CREATE OR REPLACE TABLE fact_sales AS
SELECT
  s.sale_id,

-- Dimension keys
  c.customer_id,
  p.product_id,
  w.weather_sk,

  s.sale_date,
  s.quantity,
  s.discount,

-- Frozen price
  p.price AS unit_price_at_sale,
  CASE
    WHEN w.weather_sk IS NOT NULL THEN 1
    ELSE 0
  END AS has_weather_match,
  CASE
    WHEN p.is_price_valid = 1 THEN 1
    ELSE 0
  END AS has_valid_price,

-- Amounts
  CASE
    WHEN p.is_price_valid = 1 AND p.price IS NOT NULL THEN CAST(ROUND((s.quantity * p.price), 2) AS DECIMAL(18,2))
    ELSE NULL
  END AS gross_amount,
  CASE
    WHEN p.is_price_valid = 1 AND p.price IS NOT NULL THEN CAST(ROUND((s.quantity * p.price) * (s.discount / 100.0), 2) AS DECIMAL(18,2))
    ELSE NULL
  END AS discount_amount,
  CASE
    WHEN p.is_price_valid = 1 AND p.price IS NOT NULL THEN CAST(ROUND((s.quantity * p.price) * (1 - s.discount / 100.0), 2) AS DECIMAL(18,2))
    ELSE NULL
  END AS net_amount

FROM stg_sales s
-- INVARIANT: customer_id and product_id referential integrity is enforced
-- upstream by post_clean_checks(), so clean sales rows must already be free
-- of orphan customer/product keys before this load step runs.
JOIN dim_customers c
  ON s.customer_id = c.customer_id
JOIN dim_products p
  ON s.product_id = p.product_id
LEFT JOIN dim_weather w
  ON w.date = s.sale_date
 AND w.city = c.city
ORDER BY s.sale_id ASC;


-- =========================
-- 4) MART
-- =========================

CREATE OR REPLACE TABLE weather_sales_mart AS
WITH mart_base AS (
  SELECT
    f.sale_date,
    c.city,
    p.category,
    SUM(f.quantity) AS total_units,
    COUNT(*) AS num_orders,
    SUM(f.has_valid_price) AS num_orders_priced,
    ROUND(
      SUM(CASE WHEN f.has_valid_price = 1 THEN f.net_amount ELSE 0 END),
      2
    ) AS total_revenue,
    ROUND(
      SUM(CASE WHEN f.has_valid_price = 1 THEN f.net_amount ELSE 0 END)
      / NULLIF(SUM(f.has_valid_price), 0),
      2
    ) AS avg_ticket,
    AVG(w.temp_c) AS avg_temp_c,
    AVG(w.precip_mm) AS avg_precip_mm,
    AVG(w.precip_hours) AS avg_precip_hours,
    ROUND(100.0 * SUM(f.has_valid_price) / NULLIF(COUNT(*), 0), 2) AS valid_price_rate,
    -- INVARIANT: dim_weather has at most one row per (date, city), so each
    -- (sale_date, city, category) group sees a single weather bucket value.
    ANY_VALUE(w.rain_bucket) AS rain_bucket,
    ANY_VALUE(w.temp_bucket) AS temp_bucket
  FROM fact_sales f
  JOIN dim_customers c
    ON f.customer_id = c.customer_id
  JOIN dim_products p
    ON f.product_id = p.product_id
  LEFT JOIN dim_weather w
    ON f.weather_sk = w.weather_sk
  -- Business rule: the mart is city-grained, so fact rows whose customer city
  -- is still unknown are intentionally kept in fact_sales but excluded here.
  WHERE c.city IS NOT NULL
  GROUP BY 1,2,3
)
SELECT
  sale_date,
  city,
  category,
  total_units,
  num_orders,
  num_orders_priced,
  total_revenue,
  avg_ticket,
  avg_temp_c,
  avg_precip_mm,
  avg_precip_hours,
  valid_price_rate,
  rain_bucket,
  temp_bucket
FROM mart_base
ORDER BY sale_date ASC;


-- =========================
-- 5) RUNTIME INVARIANTS
-- =========================
-- Fail the load if silver grain or join assumptions broke.
-- These run in DuckDB itself, so a hand-executed .sql is also protected.

SELECT CASE
  WHEN (SELECT COUNT(*) FROM dim_customers)
    <> (SELECT COUNT(DISTINCT customer_id) FROM dim_customers)
  THEN error('INVARIANT: dim_customers is not unique on customer_id')
END;

SELECT CASE
  WHEN (SELECT COUNT(*) FROM dim_products)
    <> (SELECT COUNT(DISTINCT product_id) FROM dim_products)
  THEN error('INVARIANT: dim_products is not unique on product_id')
END;

SELECT CASE
  WHEN (SELECT COUNT(*) FROM dim_weather WHERE weather_sk IS NULL) > 0
  THEN error('INVARIANT: dim_weather.weather_sk is null')
END;

SELECT CASE
  WHEN (SELECT COUNT(*) FROM dim_weather)
    <> (SELECT COUNT(DISTINCT weather_sk) FROM dim_weather)
  THEN error('INVARIANT: dim_weather is not unique on weather_sk')
END;

SELECT CASE
  WHEN (SELECT COUNT(*) FROM dim_weather)
    <> (SELECT COUNT(DISTINCT CAST(date AS VARCHAR) || '|' || city) FROM dim_weather)
  THEN error('INVARIANT: dim_weather is not unique on (date, city)')
END;

SELECT CASE
  WHEN (SELECT COUNT(*) FROM fact_sales)
    <> (SELECT COUNT(DISTINCT sale_id) FROM fact_sales)
  THEN error('INVARIANT: fact_sales is not unique on sale_id')
END;

SELECT CASE
  WHEN (
    SELECT COUNT(*)
    FROM fact_sales f
    LEFT JOIN dim_customers c
      ON f.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
  ) > 0
  THEN error('INVARIANT: fact_sales has orphan customer_id')
END;

SELECT CASE
  WHEN (
    SELECT COUNT(*)
    FROM fact_sales f
    LEFT JOIN dim_products p
      ON f.product_id = p.product_id
    WHERE p.product_id IS NULL
  ) > 0
  THEN error('INVARIANT: fact_sales has orphan product_id')
END;

SELECT CASE
  WHEN (
    SELECT COUNT(*)
    FROM weather_sales_mart
    WHERE num_orders_priced > num_orders
  ) > 0
  THEN error('INVARIANT: num_orders_priced exceeds num_orders')
END;
