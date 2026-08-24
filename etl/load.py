from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter
from typing import Any

import duckdb

from etl.settings import get_settings

logger = logging.getLogger("etl.load")

MART_ROWS_ATTR = "mart_rows"
MART_COLS_ATTR = "mart_cols"

SUPPORTED_EXPORT_FORMATS = ("csv", "parquet")
DEFAULT_MART_EXPORT_FORMATS = ("csv", "parquet")
DEFAULT_STAR_SCHEMA_EXPORT_FORMATS = ("parquet",)
STAR_SCHEMA_EXPORT_QUERIES = {
    "dim_customers": "SELECT * FROM dim_customers ORDER BY customer_id ASC",
    "dim_products": "SELECT * FROM dim_products ORDER BY product_id ASC",
    "dim_weather": "SELECT * FROM dim_weather ORDER BY date ASC, city ASC",
    "fact_sales": "SELECT * FROM fact_sales ORDER BY sale_id ASC",
}


def mart_lineage_counts(mart_df: Any) -> dict[str, dict[str, int]]:
    """Split published mart size from the LIMIT 10 preview returned by load."""
    preview_rows = int(mart_df.shape[0])
    preview_cols = int(mart_df.shape[1])
    attrs = getattr(mart_df, "attrs", {}) or {}
    return {
        "mart": {
            "rows": int(attrs.get(MART_ROWS_ATTR, preview_rows)),
            "cols": int(attrs.get(MART_COLS_ATTR, preview_cols)),
        },
        "mart_preview": {"rows": preview_rows, "cols": preview_cols},
    }


def _set_duckdb_variable(
    con: duckdb.DuckDBPyConnection, var_name: str, value: str | float
) -> None:
    con.execute(f"SET VARIABLE {var_name} = ?", [value])


def _set_duckdb_path_variable(
    con: duckdb.DuckDBPyConnection, var_name: str, path: Path
) -> None:
    _set_duckdb_variable(con, var_name, path.resolve().as_posix())


def _get_analytics_bucket_variables(settings: dict[str, Any]) -> dict[str, float]:
    analytics_cfg = settings.get("analytics", {})
    buckets_cfg = (
        analytics_cfg.get("buckets", {}) if isinstance(analytics_cfg, dict) else {}
    )
    rain_cfg = buckets_cfg.get("rain", {}) if isinstance(buckets_cfg, dict) else {}
    temperature_cfg = (
        buckets_cfg.get("temperature", {}) if isinstance(buckets_cfg, dict) else {}
    )

    light_rain_lt_mm = float(rain_cfg.get("light_rain_lt_mm", 2.0))
    cold_lt_c = float(temperature_cfg.get("cold_lt_c", 10.0))
    mild_lte_c = float(temperature_cfg.get("mild_lte_c", 25.0))

    if light_rain_lt_mm < 0:
        raise ValueError("[LOAD] analytics.buckets.rain.light_rain_lt_mm must be >= 0")
    if cold_lt_c > mild_lte_c:
        raise ValueError(
            "[LOAD] analytics.buckets.temperature.cold_lt_c must be <= mild_lte_c"
        )

    return {
        "rain_light_rain_lt_mm": light_rain_lt_mm,
        "temp_cold_lt_c": cold_lt_c,
        "temp_mild_lte_c": mild_lte_c,
    }


def _normalize_export_formats(
    raw_formats: Any,
    *,
    default: tuple[str, ...],
    setting_name: str,
) -> tuple[str, ...]:
    """Validate configured export formats and normalize them to a unique tuple."""

    if raw_formats is None:
        return default

    if not isinstance(raw_formats, (list, tuple)):
        raise ValueError(f"[LOAD] {setting_name} must be a list of export formats")

    normalized: list[str] = []
    for raw_format in raw_formats:
        if not isinstance(raw_format, str) or not raw_format.strip():
            raise ValueError(f"[LOAD] {setting_name} entries must be non-empty strings")

        export_format = raw_format.strip().lower()
        if export_format not in SUPPORTED_EXPORT_FORMATS:
            raise ValueError(
                f"[LOAD] {setting_name} contains unsupported format: {export_format}"
            )

        if export_format not in normalized:
            normalized.append(export_format)

    if not normalized:
        raise ValueError(f"[LOAD] {setting_name} must contain at least one format")

    return tuple(normalized)


def get_export_formats(
    settings: dict[str, Any] | None = None,
) -> dict[str, tuple[str, ...]]:
    """Resolve the effective export formats for mart and star-schema outputs."""

    resolved_settings = settings if settings is not None else get_settings()
    exports_cfg = (
        resolved_settings.get("exports", {})
        if isinstance(resolved_settings, dict)
        else {}
    )

    return {
        "mart": _normalize_export_formats(
            exports_cfg.get("mart_formats") if isinstance(exports_cfg, dict) else None,
            default=DEFAULT_MART_EXPORT_FORMATS,
            setting_name="exports.mart_formats",
        ),
        "star_schema": _normalize_export_formats(
            (
                exports_cfg.get("star_schema_formats")
                if isinstance(exports_cfg, dict)
                else None
            ),
            default=DEFAULT_STAR_SCHEMA_EXPORT_FORMATS,
            setting_name="exports.star_schema_formats",
        ),
    }


def get_output_artifact_paths(
    settings: dict[str, Any] | None = None,
) -> dict[str, Path]:
    """Return the output artifact paths implied by current settings and export formats."""

    resolved_settings = settings if settings is not None else get_settings()
    paths = (
        resolved_settings.get("paths", {})
        if isinstance(resolved_settings, dict)
        else {}
    )
    export_formats = get_export_formats(resolved_settings)

    silver_dir = Path(str(paths.get("silver_dir", "data/processed/silver")))
    rejected_dir = Path(str(paths.get("rejected_dir", "data/processed/rejected")))
    gold_csv_dir = Path(str(paths.get("gold_csv_dir", "data/processed/gold/csv")))
    gold_parquet_dir = Path(
        str(paths.get("gold_parquet_dir", "data/processed/gold/parquet"))
    )
    duckdb_path = Path(str(paths.get("duckdb_path", "warehouse.duckdb")))

    artifacts: dict[str, Path] = {
        "silver_customers": silver_dir / "customers_clean.parquet",
        "silver_products": silver_dir / "products_clean.parquet",
        "silver_sales": silver_dir / "sales_clean.parquet",
        "silver_weather": silver_dir / "weather_daily_clean.parquet",
        "rejected_sales": rejected_dir / "sales_rejected.csv",
        "duckdb_database": duckdb_path,
    }

    if "csv" in export_formats["mart"]:
        artifacts["gold_mart_csv"] = gold_csv_dir / "marts" / "weather_sales_mart.csv"
    if "parquet" in export_formats["mart"]:
        artifacts["gold_mart_parquet"] = (
            gold_parquet_dir / "marts" / "weather_sales_mart.parquet"
        )

    for table_name in STAR_SCHEMA_EXPORT_QUERIES:
        if "csv" in export_formats["star_schema"]:
            artifacts[f"gold_{table_name}_csv"] = (
                gold_csv_dir / "star_schema" / f"{table_name}.csv"
            )
        if "parquet" in export_formats["star_schema"]:
            artifacts[f"gold_{table_name}_parquet"] = (
                gold_parquet_dir / "star_schema" / f"{table_name}.parquet"
            )

    return artifacts


def _export_query(
    con: duckdb.DuckDBPyConnection,
    export_query: str,
    *,
    out_path: Path,
    export_format: str,
) -> None:
    """Export a SQL query result to a single CSV or Parquet destination."""

    if export_format == "parquet":
        con.execute(
            f"""
            COPY ({export_query})
            TO ?
            (FORMAT PARQUET);
            """,
            [out_path.as_posix()],
        )
        return

    if export_format == "csv":
        con.execute(
            f"""
            COPY ({export_query})
            TO ?
            (FORMAT CSV, HEADER TRUE);
            """,
            [out_path.as_posix()],
        )
        return

    raise ValueError(f"[LOAD] unsupported export format: {export_format}")


def build_analytics_model(
    mart_sql_path: str | None = None,
    db_path: str | None = None,
):
    """Build the DuckDB star schema and mart, then export configured gold artifacts."""

    started_at = perf_counter()
    settings = get_settings()
    paths = settings.get("paths", {})

    sql_model_path = Path(
        mart_sql_path or paths.get("sql_model", "sql/build_analytics_model.sql")
    )
    db_file = Path(db_path or paths.get("duckdb_path", "warehouse.duckdb"))

    gold_csv_root = Path(paths.get("gold_csv_dir", "data/processed/gold/csv"))
    gold_parquet_root = Path(
        paths.get("gold_parquet_dir", "data/processed/gold/parquet")
    )
    silver_dir = Path(paths.get("silver_dir", "data/processed/silver"))
    export_formats = get_export_formats(settings)

    marts_csv_dir = gold_csv_root / "marts"
    marts_parquet_dir = gold_parquet_root / "marts"
    star_csv_dir = gold_csv_root / "star_schema"
    star_parquet_dir = gold_parquet_root / "star_schema"

    logger.info(
        "[LOAD] Starting build_analytics_model sql=%s db=%s",
        sql_model_path,
        db_file,
    )

    # Ordered GOLD folders
    if "csv" in export_formats["mart"]:
        marts_csv_dir.mkdir(parents=True, exist_ok=True)
    if "parquet" in export_formats["mart"]:
        marts_parquet_dir.mkdir(parents=True, exist_ok=True)
    if "csv" in export_formats["star_schema"]:
        star_csv_dir.mkdir(parents=True, exist_ok=True)
    if "parquet" in export_formats["star_schema"]:
        star_parquet_dir.mkdir(parents=True, exist_ok=True)

    if db_file.parent != Path("."):
        db_file.parent.mkdir(parents=True, exist_ok=True)

    if sql_model_path.suffix.lower() != ".sql":
        msg = f"[LOAD] SQL file must end with .sql: {sql_model_path}"
        logger.error(msg)
        raise ValueError(msg)

    if not sql_model_path.exists():
        msg = f"[LOAD] SQL file not found: {sql_model_path}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    try:
        with duckdb.connect(str(db_file)) as con:
            # Execute main SQL model (creates stg, dims, fact, mart)
            sql_text = sql_model_path.read_text(encoding="utf-8")
            _set_duckdb_path_variable(
                con, "silver_customers_path", silver_dir / "customers_clean.parquet"
            )
            _set_duckdb_path_variable(
                con, "silver_products_path", silver_dir / "products_clean.parquet"
            )
            _set_duckdb_path_variable(
                con, "silver_sales_path", silver_dir / "sales_clean.parquet"
            )
            _set_duckdb_path_variable(
                con, "silver_weather_path", silver_dir / "weather_daily_clean.parquet"
            )
            for var_name, value in _get_analytics_bucket_variables(settings).items():
                _set_duckdb_variable(con, var_name, value)

            con.execute(sql_text)
            logger.info("[LOAD] SQL model executed")

            # ---------- EXPORT MART (only 1) ----------
            mart_export_query = "SELECT * FROM weather_sales_mart"
            mart_destinations: list[tuple[str, Path]] = []
            if "csv" in export_formats["mart"]:
                mart_destinations.append(
                    ("csv", (marts_csv_dir / "weather_sales_mart.csv").resolve())
                )
            if "parquet" in export_formats["mart"]:
                mart_destinations.append(
                    (
                        "parquet",
                        (marts_parquet_dir / "weather_sales_mart.parquet").resolve(),
                    )
                )

            for export_format, out_path in mart_destinations:
                _export_query(
                    con,
                    mart_export_query,
                    out_path=out_path,
                    export_format=export_format,
                )
            logger.info(
                "[LOAD] Exported weather_sales_mart formats=%s",
                ",".join(export_formats["mart"]),
            )

            # ---------- EXPORT STAR SCHEMA ----------
            for table_name, export_query in STAR_SCHEMA_EXPORT_QUERIES.items():
                for export_format in export_formats["star_schema"]:
                    out_dir = (
                        star_csv_dir if export_format == "csv" else star_parquet_dir
                    )
                    out_path = (
                        out_dir
                        / f"{table_name}.{'csv' if export_format == 'csv' else 'parquet'}"
                    ).resolve()
                    _export_query(
                        con,
                        export_query,
                        out_path=out_path,
                        export_format=export_format,
                    )
                logger.debug(
                    "[LOAD] Exported table=%s formats=%s",
                    table_name,
                    ",".join(export_formats["star_schema"]),
                )

            # COUNT is the published mart size. LIMIT 10 is only a glance at columns.
            mart_rows = int(
                con.execute("SELECT COUNT(*) FROM weather_sales_mart").fetchone()[0]
            )
            mart_df = con.execute("SELECT * FROM weather_sales_mart LIMIT 10").fetchdf()
            mart_cols = int(mart_df.shape[1])
            mart_df.attrs[MART_ROWS_ATTR] = mart_rows
            mart_df.attrs[MART_COLS_ATTR] = mart_cols
            logger.info(
                "[LOAD] build_analytics_model finished mart_rows=%s mart_cols=%s "
                "preview_rows=%s duration_s=%.3f",
                mart_rows,
                mart_cols,
                mart_df.shape[0],
                perf_counter() - started_at,
            )
            return mart_df

    except Exception:
        logger.exception(
            "[LOAD] build_analytics_model failed duration_s=%.3f",
            perf_counter() - started_at,
        )
        raise
