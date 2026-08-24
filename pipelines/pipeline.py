import json
import logging
import os
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from etl.alerts import send_alert
from etl.extract import extract_csv, extract_weather_api, get_sources
from etl.lineage import (
    assemble_lineage_report,
    build_observability_summary,
    collect_artifact_fingerprints,
    utc_now_iso,
    write_lineage_report,
)
from etl.load import (
    build_analytics_model,
    get_output_artifact_paths,
    mart_lineage_counts,
)
from etl.settings import get_settings
from etl.transform import profiling, transform
from etl.validations import QualityGateError, enforce_quality_report, post_clean_checks


def setup_logging(
    log_file: str | None = None,
    level: str | None = None,
    max_bytes: int | None = None,
    backup_count: int | None = None,
) -> None:
    """Configure logging once for console and file output."""
    settings = get_settings()
    logging_cfg_raw = settings.get("logging", {}) if isinstance(settings, dict) else {}
    logging_cfg = logging_cfg_raw if isinstance(logging_cfg_raw, dict) else {}

    resolved_log_file = str(
        log_file or logging_cfg.get("file", "logs/etl/pipeline.log")
    )
    resolved_level = str(
        level or os.getenv("LOG_LEVEL") or logging_cfg.get("level", "INFO")
    )
    resolved_max_bytes = int(
        max_bytes if max_bytes is not None else logging_cfg.get("max_bytes", 2_000_000)
    )
    resolved_backup_count = int(
        backup_count if backup_count is not None else logging_cfg.get("backup_count", 5)
    )

    # Root logger captures events from all module loggers.
    root_logger = logging.getLogger()
    log_level = getattr(logging, resolved_level.upper(), logging.INFO)
    root_logger.setLevel(log_level)

    # Common format for console and file.
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    has_console = any(
        getattr(h, "name", "") == "etl_console" for h in root_logger.handlers
    )
    has_file = any(getattr(h, "name", "") == "etl_file" for h in root_logger.handlers)

    # Console handler: instant feedback while the pipeline runs.
    if not has_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        console_handler.name = "etl_console"
        root_logger.addHandler(console_handler)

    # File handler with rotation: preserve previous logs and rotate by size.
    if not has_file:
        log_path = Path(resolved_log_file)
        if log_path.parent != Path("."):
            log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=resolved_max_bytes,
            backupCount=resolved_backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        file_handler.name = "etl_file"
        root_logger.addHandler(file_handler)


def _log_dataset_shapes(
    logger: logging.Logger, datasets: dict[str, Any], stage: str
) -> None:
    """Log rows and columns for each dataset in a dictionary."""
    for name, df in datasets.items():
        if hasattr(df, "shape"):
            # Most ETL outputs here are DataFrames, so shape is useful.
            logger.info(
                "[%s] %s -> rows=%s cols=%s", stage, name, df.shape[0], df.shape[1]
            )
        else:
            # Fallback in case a stage returns a non-DataFrame object.
            logger.info("[%s] %s -> type=%s", stage, name, type(df).__name__)


def _stage_start(lineage: dict[str, Any], stage_name: str) -> float:
    """Mark a stage start in lineage and return a monotonic timer baseline."""

    stage_entry = lineage["stages"].setdefault(stage_name, {})
    stage_entry["started_at_utc"] = utc_now_iso()
    return time.perf_counter()


def _stage_end(lineage: dict[str, Any], stage_name: str, stage_start: float) -> None:
    """Mark a stage end in lineage and persist its elapsed duration."""

    stage_entry = lineage["stages"].setdefault(stage_name, {})
    stage_entry["ended_at_utc"] = utc_now_iso()
    stage_entry["duration_seconds"] = round(time.perf_counter() - stage_start, 6)


def _build_output_artifacts() -> dict[str, Path]:
    return get_output_artifact_paths()


def _build_lineage_skeleton(run_id: str, fetch_weather: bool) -> dict[str, Any]:
    """Build the initial lineage payload populated during the pipeline run."""
    return {
        "run_id": run_id,
        "pipeline": "retail-weather-etl",
        "fetch_weather": fetch_weather,
        "started_at_utc": utc_now_iso(),
        "status": "running",
        "stages": {},
        "row_counts": {},
        "artifacts": {},
    }


def _latest_stage_name(lineage: dict[str, Any]) -> str | None:
    stages_obj = lineage.get("stages", {})
    if not isinstance(stages_obj, dict) or not stages_obj:
        return None
    stages: dict[str, Any] = stages_obj
    latest_stage = next(reversed(stages))
    return str(latest_stage)


def run_pipeline(fetch_weather: bool = True):
    """Run the full ETL pipeline and return a preview of the final mart."""

    # 1) Initialize logging first, so every step is traceable.
    setup_logging()

    logger = logging.getLogger("etl.pipeline")
    # Global timer for full pipeline duration.
    total_start = time.perf_counter()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    lineage_report = _build_lineage_skeleton(run_id, fetch_weather)
    logger.info("=== START PIPELINE ===")

    try:
        sources = get_sources()
        output_artifacts = _build_output_artifacts()
        lineage_report["artifacts"]["sources_pre_extract"] = (
            collect_artifact_fingerprints(sources)
        )

        # 2) Extract weather data from API (optional for offline reproducibility).
        if fetch_weather:
            stage_start = _stage_start(lineage_report, "extract_weather")
            extract_weather_api()
            _stage_end(lineage_report, "extract_weather", stage_start)
            logger.info(
                "[EXTRACT] weather API completed in %.2fs",
                time.perf_counter() - stage_start,
            )
        else:
            stage_start = _stage_start(lineage_report, "extract_weather")
            weather_path = sources["weather_daily"]
            if not weather_path.exists():
                raise FileNotFoundError(
                    f"[EXTRACT] fetch_weather=False but file not found: {weather_path}"
                )
            logger.info(
                "[EXTRACT] Skipping weather API; using existing %s", weather_path
            )
            _stage_end(lineage_report, "extract_weather", stage_start)

        # 3) Extract all CSV sources listed in current config.
        stage_start = _stage_start(lineage_report, "extract_csv")
        logger.info("[EXTRACT] loading %d CSV sources", len(sources))
        results = extract_csv(sources)
        lineage_report["row_counts"]["extract"] = {
            name: int(df.shape[0]) for name, df in results.items()
        }
        _log_dataset_shapes(logger, results, "EXTRACT")
        _stage_end(lineage_report, "extract_csv", stage_start)
        logger.info(
            "[EXTRACT] CSV extraction completed in %.2fs",
            time.perf_counter() - stage_start,
        )
        lineage_report["artifacts"]["sources_post_extract"] = (
            collect_artifact_fingerprints(sources)
        )

        # 4) Run raw-data profiling checks and summaries.
        stage_start = _stage_start(lineage_report, "profiling")
        profiling(results, detail=False)
        _stage_end(lineage_report, "profiling", stage_start)
        logger.info("[PROFILE] completed in %.2fs", time.perf_counter() - stage_start)

        # 5) Clean data and split valid vs rejected records.
        stage_start = _stage_start(lineage_report, "transform")
        cleaned, rejected = transform(results, run_checks=False)
        lineage_report["row_counts"]["cleaned"] = {
            name: int(df.shape[0]) for name, df in cleaned.items()
        }
        lineage_report["row_counts"]["rejected"] = {
            name: int(df.shape[0]) for name, df in rejected.items()
        }
        _log_dataset_shapes(logger, cleaned, "TRANSFORM_CLEAN")
        _log_dataset_shapes(logger, rejected, "TRANSFORM_REJECTED")
        validation_report = post_clean_checks(cleaned, rejected, fail_on_critical=False)
        lineage_report["quality_report"] = validation_report
        logger.info(
            "[QUALITY] report=%s",
            json.dumps(validation_report, ensure_ascii=False, sort_keys=True),
        )
        enforce_quality_report(validation_report)
        _stage_end(lineage_report, "transform", stage_start)
        logger.info("[TRANSFORM] completed in %.2fs", time.perf_counter() - stage_start)
        lineage_report["artifacts"]["silver_rejected_outputs"] = (
            collect_artifact_fingerprints(
                {
                    "silver_customers": output_artifacts["silver_customers"],
                    "silver_products": output_artifacts["silver_products"],
                    "silver_sales": output_artifacts["silver_sales"],
                    "silver_weather": output_artifacts["silver_weather"],
                    "rejected_sales": output_artifacts["rejected_sales"],
                }
            )
        )

        # 6) Build dimensional model + mart in DuckDB/export layer.
        stage_start = _stage_start(lineage_report, "load")
        mart_df = build_analytics_model()
        mart_counts = mart_lineage_counts(mart_df)
        lineage_report["row_counts"]["mart"] = mart_counts["mart"]
        lineage_report["row_counts"]["mart_preview"] = mart_counts["mart_preview"]
        _stage_end(lineage_report, "load", stage_start)
        logger.info(
            "[LOAD] mart -> rows=%s cols=%s preview_rows=%s",
            mart_counts["mart"]["rows"],
            mart_counts["mart"]["cols"],
            mart_counts["mart_preview"]["rows"],
        )
        logger.info("[LOAD] completed in %.2fs", time.perf_counter() - stage_start)
        lineage_report["artifacts"]["final_outputs"] = collect_artifact_fingerprints(
            output_artifacts
        )

        # 7) Final success log with total runtime.
        lineage_report["status"] = "passed"
        logger.info(
            "=== PIPELINE FINISHED OK (%.2fs) ===", time.perf_counter() - total_start
        )
        return mart_df
    except QualityGateError as exc:
        lineage_report["status"] = "failed_quality_gate"
        lineage_report["quality_report"] = exc.report
        lineage_report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        send_alert(
            title="Quality gate failed",
            message=(
                "Critical validation checks failed. Silver and rejected outputs "
                "may already exist for inspection; the gold load did not run."
            ),
            severity="high",
            context={
                "run_id": run_id,
                "status": lineage_report["status"],
                "stage": _latest_stage_name(lineage_report),
                "error_type": type(exc).__name__,
                "critical_failures": "; ".join(
                    str(item) for item in exc.report.get("critical_failures", [])[:3]
                ),
                "fetch_weather": fetch_weather,
            },
        )
        logger.error(
            "[QUALITY] failed_report=%s",
            json.dumps(exc.report, ensure_ascii=False, sort_keys=True),
        )
        logger.exception("=== PIPELINE FAILED (QUALITY GATE) ===")
        raise
    except Exception as exc:
        lineage_report["status"] = "failed"
        lineage_report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        send_alert(
            title="Pipeline failed",
            message="Unhandled exception in ETL pipeline.",
            severity="high",
            context={
                "run_id": run_id,
                "status": lineage_report["status"],
                "stage": _latest_stage_name(lineage_report),
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "fetch_weather": fetch_weather,
            },
        )
        # logger.exception prints error message + traceback.
        logger.exception("=== PIPELINE FAILED ===")
        # Re-raise so orchestrators/CI can detect failure.
        raise
    finally:
        row_counts = lineage_report.get("row_counts", {})
        lineage_report = assemble_lineage_report(
            run_id=run_id,
            fetch_weather=fetch_weather,
            status=str(lineage_report.get("status", "failed")),
            started_at_utc=str(lineage_report["started_at_utc"]),
            ended_at_utc=utc_now_iso(),
            stages=lineage_report.get("stages"),
            extract_rows=row_counts.get("extract"),
            cleaned_rows=row_counts.get("cleaned"),
            rejected_rows=row_counts.get("rejected"),
            mart=row_counts.get("mart"),
            mart_preview=row_counts.get("mart_preview"),
            artifacts=lineage_report.get("artifacts"),
            quality_report=lineage_report.get("quality_report"),
            error=lineage_report.get("error"),
            duration_seconds=round(time.perf_counter() - total_start, 6),
            orchestration="local",
        )
        try:
            output = write_lineage_report(lineage_report)
            logger.info("[LINEAGE] report=%s", output)
            summary = build_observability_summary(lineage_report)
            summary["lineage_path"] = str(output)
            logger.info(
                "[OBSERVABILITY] summary=%s",
                json.dumps(summary, ensure_ascii=False, sort_keys=True),
            )
        except Exception:
            logger.exception("[LINEAGE] failed to persist report")


if __name__ == "__main__":
    # Script entrypoint: run pipeline only when executed directly.
    run_pipeline()
