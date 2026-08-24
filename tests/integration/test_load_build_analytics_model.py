import hashlib
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from etl.load import build_analytics_model
from etl.settings import clear_settings_cache
from etl.transform import save_silver_and_rejected


def _empty_rejected_sales() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sale_id": pd.Series([], dtype="int64"),
            "customer_id": pd.Series([], dtype="int64"),
            "product_id": pd.Series([], dtype="int64"),
            "sale_date": pd.Series([], dtype="datetime64[ns]"),
            "quantity": pd.Series([], dtype="Int64"),
            "discount": pd.Series([], dtype="float64"),
            "reject_reasons": pd.Series([], dtype="string"),
        }
    )


def _bucket_test_outputs() -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    dates = pd.to_datetime(["2024-03-01", "2024-03-02", "2024-03-03", "2024-03-04"])
    cleaned = {
        "customers": pd.DataFrame(
            {
                "customer_id": [1, 2, 3, 4],
                "first_name": ["Ana", "Luis", "Eva", "Pau"],
                "last_name": ["Lopez", "Garcia", "Martinez", "Soler"],
                "city": ["Madrid", "Madrid", "Madrid", "Madrid"],
                "signup_date": pd.to_datetime(
                    ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
                ),
            }
        ),
        "products": pd.DataFrame(
            {
                "product_id": [1001],
                "product_name": ["Bottle"],
                "category": ["Accessories"],
                "price": [10.0],
                "is_price_valid": [1],
            }
        ),
        "sales": pd.DataFrame(
            {
                "sale_id": [101, 102, 103, 104],
                "customer_id": [1, 2, 3, 4],
                "product_id": [1001, 1001, 1001, 1001],
                "sale_date": dates,
                "quantity": pd.Series([2, 1, 1, 1], dtype="Int64"),
                "discount": [10.0, 0.0, 0.0, 0.0],
            }
        ),
        "weather_daily": pd.DataFrame(
            {
                "date": dates,
                "city": ["Madrid", "Madrid", "Madrid", "Madrid"],
                "temp_c": [9.9, 10.0, 25.0, 25.1],
                "precip_mm": [0.0, 1.99, 2.0, 0.5],
                "precip_hours": [0.0, 1.0, 2.0, 1.0],
                "weather_code": pd.Series([1, 2, 3, 4], dtype="Int64"),
            }
        ),
    }
    rejected = {"sales": _empty_rejected_sales()}
    return cleaned, rejected


def _dimension_contract_test_outputs() -> (
    tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]
):
    cleaned, rejected = _bucket_test_outputs()
    cleaned["customers"] = pd.DataFrame(
        {
            "customer_id": [1, 2],
            "first_name": ["Ana", "Luis"],
            "last_name": ["Lopez", "Garcia"],
            "city": ["Madrid", "Barcelona"],
            "signup_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
        }
    )
    cleaned["products"] = pd.DataFrame(
        {
            "product_id": [1001, 1002],
            "product_name": ["Bottle", "Trail Shoes"],
            "category": ["Accessories", "Footwear"],
            "price": [10.0, pd.NA],
            "is_price_valid": [1, 0],
        }
    )
    cleaned["sales"] = pd.DataFrame(
        {
            "sale_id": [101, 102],
            "customer_id": [1, 2],
            "product_id": [1001, 1001],
            "sale_date": pd.to_datetime(["2024-03-01", "2024-03-02"]),
            "quantity": pd.Series([2, 1], dtype="Int64"),
            "discount": [10.0, 0.0],
        }
    )
    cleaned["weather_daily"] = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-03-01", "2024-03-02"]),
            "city": ["Madrid", "Barcelona"],
            "temp_c": [9.9, 10.0],
            "precip_mm": [0.0, 1.0],
            "precip_hours": [0.0, 1.0],
            "weather_code": pd.Series([1, 2], dtype="Int64"),
        }
    )
    return cleaned, rejected


def _sql_invariant_test_outputs() -> (
    tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]
):
    cleaned, rejected = _dimension_contract_test_outputs()
    cleaned["sales"] = pd.DataFrame(
        {
            "sale_id": [101, 102],
            "customer_id": [1, 2],
            "product_id": [1001, 1002],
            "sale_date": pd.to_datetime(["2024-03-01", "2024-03-02"]),
            "quantity": pd.Series([2, 1], dtype="Int64"),
            "discount": [10.0, 0.0],
        }
    )
    return cleaned, rejected


def _unknown_bucket_test_outputs() -> (
    tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]
):
    cleaned = {
        "customers": pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "first_name": ["Ana", "Luis", "Eva"],
                "last_name": ["Lopez", "Garcia", "Martinez"],
                "city": ["Madrid", "Madrid", "Barcelona"],
                "signup_date": pd.to_datetime(
                    ["2024-01-01", "2024-01-02", "2024-01-03"]
                ),
            }
        ),
        "products": pd.DataFrame(
            {
                "product_id": [1001],
                "product_name": ["Bottle"],
                "category": ["Accessories"],
                "price": [10.0],
                "is_price_valid": [1],
            }
        ),
        "sales": pd.DataFrame(
            {
                "sale_id": [101, 102, 103],
                "customer_id": [1, 2, 3],
                "product_id": [1001, 1001, 1001],
                "sale_date": pd.to_datetime(["2024-03-01", "2024-03-02", "2024-03-03"]),
                "quantity": pd.Series([1, 1, 1], dtype="Int64"),
                "discount": [0.0, 0.0, 0.0],
            }
        ),
        "weather_daily": pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-03-01", "2024-03-02"]),
                "city": ["Madrid", "Madrid"],
                # Independent nulls: rain unknown + mild, then no_rain + temp unknown.
                "temp_c": [15.0, pd.NA],
                "precip_mm": [pd.NA, 0.0],
                "precip_hours": [pd.NA, 0.0],
                "weather_code": pd.Series([1, 2], dtype="Int64"),
            }
        ),
    }
    rejected = {"sales": _empty_rejected_sales()}
    return cleaned, rejected


@pytest.mark.integration
def test_build_analytics_model_creates_tables_and_exports(
    tmp_path, monkeypatch, transformed_outputs
):
    cleaned, rejected = transformed_outputs
    monkeypatch.chdir(tmp_path)

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    mart_df = build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))

    assert isinstance(mart_df, pd.DataFrame)
    assert not mart_df.empty
    assert {"sale_date", "total_units", "total_revenue"}.issubset(mart_df.columns)
    assert mart_df.shape[0] <= 10

    with duckdb.connect(str(db_path)) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert {
            "dim_customers",
            "dim_products",
            "dim_weather",
            "fact_sales",
            "weather_sales_mart",
        }.issubset(tables)

        fact_rows = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        assert fact_rows > 0
        mart_rows = con.execute("SELECT COUNT(*) FROM weather_sales_mart").fetchone()[0]
        assert mart_df.attrs["mart_rows"] == mart_rows
        assert mart_df.attrs["mart_cols"] == mart_df.shape[1]
        assert mart_rows >= mart_df.shape[0]

    assert not (
        tmp_path / "data/processed/gold/csv/marts/weather_sales_mart.csv"
    ).exists()
    assert (
        tmp_path / "data/processed/gold/parquet/marts/weather_sales_mart.parquet"
    ).exists()
    assert (
        tmp_path / "data/processed/gold/parquet/star_schema/dim_customers.parquet"
    ).exists()
    assert (
        tmp_path / "data/processed/gold/parquet/star_schema/dim_products.parquet"
    ).exists()
    assert (
        tmp_path / "data/processed/gold/parquet/star_schema/fact_sales.parquet"
    ).exists()
    assert not (
        tmp_path / "data/processed/gold/csv/star_schema/dim_customers.csv"
    ).exists()


@pytest.mark.integration
def test_build_analytics_model_validates_dim_weather_fact_sales_and_mart_semantics(
    tmp_path, monkeypatch
):
    cleaned, rejected = _bucket_test_outputs()
    monkeypatch.chdir(tmp_path)

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))

    with duckdb.connect(str(db_path)) as con:
        dim_weather = con.execute("""
            SELECT weather_sk, date, city, rain_bucket, temp_bucket
            FROM dim_weather
            ORDER BY date ASC
            """).fetchdf()
        expected_weather_keys = [
            hashlib.md5(f"{date}|Madrid".encode("utf-8")).hexdigest()
            for date in ["2024-03-01", "2024-03-02", "2024-03-03", "2024-03-04"]
        ]
        assert dim_weather["weather_sk"].tolist() == expected_weather_keys
        assert dim_weather["rain_bucket"].tolist() == [
            "no_rain",
            "light_rain",
            "heavy_rain",
            "light_rain",
        ]
        assert dim_weather["temp_bucket"].tolist() == [
            "cold",
            "mild",
            "mild",
            "hot",
        ]

        fact_row = con.execute("""
            SELECT
              sale_id,
              has_weather_match,
              has_valid_price,
              unit_price_at_sale,
              gross_amount,
              discount_amount,
              net_amount
            FROM fact_sales
            WHERE sale_id = 101
            """).fetchone()
        assert fact_row[:3] == (101, 1, 1)
        assert float(fact_row[3]) == pytest.approx(10.0)
        assert float(fact_row[4]) == pytest.approx(20.0)
        assert float(fact_row[5]) == pytest.approx(2.0)
        assert float(fact_row[6]) == pytest.approx(18.0)

        mart_df = con.execute("""
            SELECT
              sale_date,
              total_units,
              num_orders,
              total_revenue,
              avg_ticket,
              rain_bucket,
              temp_bucket
            FROM weather_sales_mart
            ORDER BY sale_date ASC
            """).fetchdf()

    assert mart_df["rain_bucket"].tolist() == [
        "no_rain",
        "light_rain",
        "heavy_rain",
        "light_rain",
    ]
    assert mart_df["temp_bucket"].tolist() == ["cold", "mild", "mild", "hot"]

    first_row = mart_df.iloc[0]
    assert int(first_row["total_units"]) == 2
    assert int(first_row["num_orders"]) == 1
    assert float(first_row["total_revenue"]) == pytest.approx(18.0)
    assert float(first_row["avg_ticket"]) == pytest.approx(18.0)


@pytest.mark.integration
def test_build_analytics_model_enforces_sql_invariants(tmp_path, monkeypatch):
    cleaned, rejected = _sql_invariant_test_outputs()
    monkeypatch.chdir(tmp_path)

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))

    with duckdb.connect(str(db_path)) as con:
        dim_weather_count, dim_weather_unique = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT weather_sk) FROM dim_weather"
        ).fetchone()
        fact_sales_count, fact_sales_unique = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT sale_id) FROM fact_sales"
        ).fetchone()
        orphan_customers = con.execute("""
            SELECT COUNT(*)
            FROM fact_sales f
            LEFT JOIN dim_customers c
              ON f.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            """).fetchone()[0]
        priced_order_offenders = con.execute("""
            SELECT COUNT(*)
            FROM weather_sales_mart
            WHERE num_orders_priced > num_orders
            """).fetchone()[0]
        no_price_rows = con.execute("""
            SELECT total_revenue
            FROM weather_sales_mart
            WHERE num_orders_priced = 0
            """).fetchall()

    assert dim_weather_count == dim_weather_unique
    assert fact_sales_count == fact_sales_unique
    assert orphan_customers == 0
    assert priced_order_offenders == 0
    assert all(row[0] in (0, 0.0, None) for row in no_price_rows)


@pytest.mark.integration
def test_build_analytics_model_fails_when_dim_customers_grain_is_broken(
    tmp_path, monkeypatch
):
    cleaned, rejected = _dimension_contract_test_outputs()
    duplicate = cleaned["customers"].iloc[[0]].copy()
    duplicate.loc[:, "city"] = "Valencia"
    cleaned["customers"] = pd.concat(
        [cleaned["customers"], duplicate], ignore_index=True
    )
    monkeypatch.chdir(tmp_path)

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(Exception, match="dim_customers is not unique on customer_id"):
        build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))


@pytest.mark.integration
def test_build_analytics_model_fails_when_weather_sk_is_null(tmp_path, monkeypatch):
    cleaned, rejected = _dimension_contract_test_outputs()
    cleaned["weather_daily"] = cleaned["weather_daily"].copy()
    cleaned["weather_daily"].loc[0, "city"] = pd.NA
    monkeypatch.chdir(tmp_path)

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(Exception, match="dim_weather.weather_sk is null"):
        build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))


@pytest.mark.integration
def test_build_analytics_model_preserves_dim_customers_contract(tmp_path, monkeypatch):
    cleaned, rejected = _dimension_contract_test_outputs()
    monkeypatch.chdir(tmp_path)

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))

    with duckdb.connect(str(db_path)) as con:
        dim_customers = con.execute("""
            SELECT customer_id, first_name, last_name, city, signup_date
            FROM dim_customers
            ORDER BY customer_id ASC
            """).fetchdf()

    assert dim_customers["customer_id"].tolist() == [1, 2]
    assert dim_customers["customer_id"].duplicated().sum() == 0
    assert dim_customers["city"].tolist() == ["Madrid", "Barcelona"]
    assert dim_customers["first_name"].tolist() == ["Ana", "Luis"]
    assert dim_customers["last_name"].tolist() == ["Lopez", "Garcia"]
    assert dim_customers["signup_date"].dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-01",
        "2024-01-02",
    ]


@pytest.mark.integration
def test_build_analytics_model_preserves_dim_products_contract(tmp_path, monkeypatch):
    cleaned, rejected = _dimension_contract_test_outputs()
    monkeypatch.chdir(tmp_path)

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))

    with duckdb.connect(str(db_path)) as con:
        dim_products = con.execute("""
            SELECT product_id, product_name, category, price, is_price_valid
            FROM dim_products
            ORDER BY product_id ASC
            """).fetchdf()

    assert dim_products["product_id"].tolist() == [1001, 1002]
    assert dim_products["product_id"].duplicated().sum() == 0

    product_1001 = dim_products.iloc[0]
    assert product_1001["product_name"] == "Bottle"
    assert product_1001["category"] == "Accessories"
    assert float(product_1001["price"]) == pytest.approx(10.0)
    assert int(product_1001["is_price_valid"]) == 1

    product_1002 = dim_products.iloc[1]
    assert product_1002["product_name"] == "Trail Shoes"
    assert product_1002["category"] == "Footwear"
    assert pd.isna(product_1002["price"])
    assert int(product_1002["is_price_valid"]) == 0


@pytest.mark.integration
def test_build_analytics_model_uses_bucket_thresholds_from_config(
    tmp_path, monkeypatch
):
    cleaned, rejected = _bucket_test_outputs()
    monkeypatch.chdir(tmp_path)

    config_path = tmp_path / "analytics_config.yaml"
    config_path.write_text(
        """
analytics:
  buckets:
    rain:
      light_rain_lt_mm: 2.5
    temperature:
      cold_lt_c: 10.1
      mild_lte_c: 24.9
""".strip() + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("RETAIL_ETL_CONFIG", str(config_path))
    clear_settings_cache()

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))

    with duckdb.connect(str(db_path)) as con:
        dim_weather = con.execute("""
            SELECT rain_bucket, temp_bucket
            FROM dim_weather
            ORDER BY date ASC
            """).fetchdf()

    assert dim_weather["rain_bucket"].tolist() == [
        "no_rain",
        "light_rain",
        "light_rain",
        "light_rain",
    ]
    assert dim_weather["temp_bucket"].tolist() == [
        "cold",
        "cold",
        "hot",
        "hot",
    ]


@pytest.mark.integration
def test_build_analytics_model_keeps_null_weather_metrics_as_unknown_buckets(
    tmp_path, monkeypatch
):
    cleaned, rejected = _unknown_bucket_test_outputs()
    monkeypatch.chdir(tmp_path)

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))

    with duckdb.connect(str(db_path)) as con:
        dim_weather = con.execute("""
            SELECT date, city, rain_bucket, temp_bucket
            FROM dim_weather
            ORDER BY date ASC
            """).fetchdf()
        mart_df = con.execute("""
            SELECT sale_date, city, rain_bucket, temp_bucket
            FROM weather_sales_mart
            ORDER BY sale_date ASC
            """).fetchdf()

    assert dim_weather["rain_bucket"].tolist() == ["unknown", "no_rain"]
    assert dim_weather["temp_bucket"].tolist() == ["mild", "unknown"]

    madrid = mart_df[mart_df["city"] == "Madrid"].sort_values("sale_date")
    assert madrid["rain_bucket"].tolist() == ["unknown", "no_rain"]
    assert madrid["temp_bucket"].tolist() == ["mild", "unknown"]

    barcelona = mart_df[mart_df["city"] == "Barcelona"].iloc[0]
    assert pd.isna(barcelona["rain_bucket"])
    assert pd.isna(barcelona["temp_bucket"])


@pytest.mark.integration
def test_build_analytics_model_respects_configured_export_formats(
    tmp_path, monkeypatch
):
    cleaned, rejected = _bucket_test_outputs()
    monkeypatch.chdir(tmp_path)

    config_path = tmp_path / "exports_config.yaml"
    config_path.write_text(
        """
exports:
  mart_formats:
    - parquet
  star_schema_formats:
    - csv
    - parquet
""".strip() + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("RETAIL_ETL_CONFIG", str(config_path))
    clear_settings_cache()

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))

    assert not (
        tmp_path / "data/processed/gold/csv/marts/weather_sales_mart.csv"
    ).exists()
    assert (
        tmp_path / "data/processed/gold/parquet/marts/weather_sales_mart.parquet"
    ).exists()
    assert (tmp_path / "data/processed/gold/csv/star_schema/dim_customers.csv").exists()
    assert (
        tmp_path / "data/processed/gold/parquet/star_schema/dim_customers.parquet"
    ).exists()


@pytest.mark.integration
def test_build_analytics_model_excludes_unknown_customer_city_from_mart(
    tmp_path, monkeypatch
):
    cleaned, rejected = _bucket_test_outputs()
    cleaned["customers"] = cleaned["customers"].copy()
    cleaned["customers"].loc[cleaned["customers"]["customer_id"] == 4, "city"] = pd.NA
    monkeypatch.chdir(tmp_path)

    save_silver_and_rejected(cleaned, rejected)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    build_analytics_model(mart_sql_path=str(sql_path), db_path=str(db_path))

    with duckdb.connect(str(db_path)) as con:
        fact_sales_rows = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        mart_df = con.execute("""
            SELECT sale_date, city, total_units
            FROM weather_sales_mart
            ORDER BY sale_date ASC
            """).fetchdf()

    assert fact_sales_rows == 4
    assert mart_df["sale_date"].dt.strftime("%Y-%m-%d").tolist() == [
        "2024-03-01",
        "2024-03-02",
        "2024-03-03",
    ]
    assert mart_df["city"].tolist() == ["Madrid", "Madrid", "Madrid"]
