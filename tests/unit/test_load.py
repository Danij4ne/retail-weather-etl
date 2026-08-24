from pathlib import Path

import pytest

import etl.load as load_module


def test_build_analytics_model_raises_when_sql_file_is_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    missing_sql = tmp_path / "does_not_exist.sql"
    db_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(FileNotFoundError, match="SQL file not found"):
        load_module.build_analytics_model(
            mart_sql_path=str(missing_sql),
            db_path=str(db_path),
        )


def test_build_analytics_model_raises_when_sql_file_has_wrong_suffix(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    wrong_sql = tmp_path / "model.txt"
    wrong_sql.write_text("SELECT 1;", encoding="utf-8")
    db_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(ValueError, match="must end with .sql"):
        load_module.build_analytics_model(
            mart_sql_path=str(wrong_sql),
            db_path=str(db_path),
        )


def test_build_analytics_model_propagates_duckdb_connect_failures(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    def _boom(*args, **kwargs):
        raise RuntimeError("duckdb connect failed")

    monkeypatch.setattr("etl.load.duckdb.connect", _boom)

    with pytest.raises(RuntimeError, match="duckdb connect failed"):
        load_module.build_analytics_model(
            mart_sql_path=str(sql_path),
            db_path=str(db_path),
        )


def test_build_analytics_model_propagates_write_failures(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / "build_analytics_model.sql"
    db_path = tmp_path / "warehouse.duckdb"

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            if "COPY (" in str(sql):
                raise OSError("duckdb write failed")
            return self

    monkeypatch.setattr("etl.load.duckdb.connect", lambda *a, **k: _FakeConnection())

    with pytest.raises(OSError, match="duckdb write failed"):
        load_module.build_analytics_model(
            mart_sql_path=str(sql_path),
            db_path=str(db_path),
        )


def test_get_output_artifact_paths_respects_configured_formats():
    settings = {
        "paths": {
            "silver_dir": "silver",
            "rejected_dir": "rejected",
            "gold_csv_dir": "gold/csv",
            "gold_parquet_dir": "gold/parquet",
            "duckdb_path": "warehouse.duckdb",
        },
        "exports": {
            "mart_formats": ["parquet"],
            "star_schema_formats": ["csv", "parquet"],
        },
    }

    artifacts = load_module.get_output_artifact_paths(settings)

    assert "gold_mart_csv" not in artifacts
    assert artifacts["gold_mart_parquet"] == Path(
        "gold/parquet/marts/weather_sales_mart.parquet"
    )
    assert artifacts["gold_dim_customers_csv"] == Path(
        "gold/csv/star_schema/dim_customers.csv"
    )
    assert artifacts["gold_dim_customers_parquet"] == Path(
        "gold/parquet/star_schema/dim_customers.parquet"
    )


def test_get_export_formats_raises_for_unsupported_format():
    settings = {
        "exports": {
            "mart_formats": ["json"],
        }
    }

    with pytest.raises(ValueError, match="exports.mart_formats"):
        load_module.get_export_formats(settings)


def test_mart_lineage_counts_prefers_attrs_over_preview_shape():
    import pandas as pd

    preview = pd.DataFrame({"sale_date": ["2025-01-01"], "total_units": [1]})
    preview.attrs["mart_rows"] = 415
    preview.attrs["mart_cols"] = 14

    counts = load_module.mart_lineage_counts(preview)

    assert counts["mart"] == {"rows": 415, "cols": 14}
    assert counts["mart_preview"] == {"rows": 1, "cols": 2}


def test_mart_lineage_counts_falls_back_to_preview_shape():
    import pandas as pd

    preview = pd.DataFrame({"sale_date": ["2025-01-01"]})

    counts = load_module.mart_lineage_counts(preview)

    assert counts["mart"] == {"rows": 1, "cols": 1}
    assert counts["mart_preview"] == {"rows": 1, "cols": 1}
