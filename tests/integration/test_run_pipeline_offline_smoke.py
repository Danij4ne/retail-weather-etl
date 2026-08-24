import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from pipelines.pipeline import run_pipeline


def _prepare_offline_workspace(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    shutil.copytree(
        project_root / "data" / "raw",
        tmp_path / "data" / "raw",
        dirs_exist_ok=True,
    )
    shutil.copytree(
        project_root / "sql",
        tmp_path / "sql",
        dirs_exist_ok=True,
    )


@pytest.mark.integration
def test_run_pipeline_offline_smoke(tmp_path, monkeypatch):
    _prepare_offline_workspace(tmp_path)

    monkeypatch.chdir(tmp_path)

    mart_df = run_pipeline(fetch_weather=False)

    assert isinstance(mart_df, pd.DataFrame)
    assert not mart_df.empty

    expected_files = [
        "data/processed/silver/customers_clean.parquet",
        "data/processed/silver/products_clean.parquet",
        "data/processed/silver/sales_clean.parquet",
        "data/processed/silver/weather_daily_clean.parquet",
        "data/processed/rejected/sales_rejected.csv",
        "data/processed/gold/parquet/marts/weather_sales_mart.parquet",
        "data/processed/gold/parquet/star_schema/dim_customers.parquet",
        "data/processed/gold/parquet/star_schema/dim_products.parquet",
        "data/processed/gold/parquet/star_schema/dim_weather.parquet",
        "data/processed/gold/parquet/star_schema/fact_sales.parquet",
    ]
    for rel_path in expected_files:
        assert (tmp_path / rel_path).exists(), f"Missing output: {rel_path}"

    lineage_reports = list((tmp_path / "logs" / "etl" / "lineage").glob("*.json"))
    assert lineage_reports, "Missing lineage report output"

    db_path = tmp_path / "warehouse.duckdb"
    assert db_path.exists()

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


@pytest.mark.integration
def test_run_pipeline_offline_is_idempotent(tmp_path, monkeypatch):
    _prepare_offline_workspace(tmp_path)
    monkeypatch.chdir(tmp_path)

    run_pipeline(fetch_weather=False)
    db_path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(db_path)) as con:
        first_full_mart = con.execute("""
            SELECT * FROM weather_sales_mart
            ORDER BY sale_date, city, category
            """).fetchdf()
        first_fact_rows = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]

    run_pipeline(fetch_weather=False)
    with duckdb.connect(str(db_path)) as con:
        second_full_mart = con.execute("""
            SELECT * FROM weather_sales_mart
            ORDER BY sale_date, city, category
            """).fetchdf()
        second_fact_rows = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]

    pd.testing.assert_frame_equal(first_full_mart, second_full_mart)
    assert first_fact_rows == second_fact_rows
