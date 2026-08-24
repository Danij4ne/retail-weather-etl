from __future__ import annotations

import json
import logging
import os
import re
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from etl.alerts import airflow_failure_callback
from etl.lineage import utc_now_iso

PROJECT_ROOT = Path("/opt/airflow/project")
PROJECT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"
TMP_STAGE_ROOT = PROJECT_ROOT / "tmp" / "staging" / "airflow"
AIRFLOW_FETCH_WEATHER_ENV = "RETAIL_ETL_FETCH_WEATHER"

# Keep ETL paths deterministic in Airflow workers without mutating cwd.
os.environ.setdefault("RETAIL_ETL_CONFIG", str(PROJECT_CONFIG))
os.environ.setdefault("RETAIL_ETL_BASE_DIR", str(PROJECT_ROOT))

_SAFE_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def _safe_identifier(value: str) -> str:
    return _SAFE_ID_RE.sub("_", value)


def _resolve_stage_dir_for_cleanup(
    stage_dir: str | Path,
    *,
    staging_root: Path,
) -> Path:
    """Return a resolved path that is a strict descendant of staging_root.

    Existence, deletion, and empty/missing stage_dir are the caller's job.
    """
    root = staging_root.resolve()
    candidate = Path(stage_dir).resolve()
    if root not in candidate.parents:
        raise ValueError(f"Refusing to cleanup outside staging root: {candidate}")
    return candidate


def _stage_report(started_at_utc: str, started_at_perf: float) -> dict[str, Any]:
    return {
        "started_at_utc": started_at_utc,
        "ended_at_utc": utc_now_iso(),
        "duration_seconds": round(time.perf_counter() - started_at_perf, 6),
    }


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default

    lowered = raw.strip().lower()
    if not lowered:
        return default

    return lowered in {"1", "true", "yes", "y", "on"}


@dag(
    dag_id="retail_weather_etl",
    description="Retail + weather ETL pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    # Silver/gold paths and the DuckDB file are fixed, not partitioned per run, so
    # overlapping runs would race on the same artifacts. Queue them instead.
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": airflow_failure_callback,
    },
    tags=["etl", "retail", "weather"],
)
def retail_weather_etl_dag():
    """Airflow DAG for the retail-weather ETL on executors sharing PROJECT_ROOT."""

    @task(task_id="extract")
    def extract_task() -> dict[str, Any]:
        from airflow.operators.python import get_current_context

        from etl.extract import extract_csv, extract_weather_api, get_sources

        stage_started_at = utc_now_iso()
        stage_start_perf = time.perf_counter()
        task_logger = logging.getLogger("airflow.task")
        sources = get_sources()
        fetch_weather = _env_flag(AIRFLOW_FETCH_WEATHER_ENV, default=False)

        if fetch_weather:
            extract_weather_api()
            task_logger.info(
                "[EXTRACT] weather API enabled via %s=true",
                AIRFLOW_FETCH_WEATHER_ENV,
            )
        else:
            weather_path = sources["weather_daily"]
            if not weather_path.exists():
                raise FileNotFoundError(
                    "[EXTRACT] Airflow weather API disabled "
                    f"({AIRFLOW_FETCH_WEATHER_ENV}=false) but file not found: "
                    f"{weather_path}"
                )
            task_logger.info(
                "[EXTRACT] weather API disabled via %s; using existing %s",
                AIRFLOW_FETCH_WEATHER_ENV,
                weather_path,
            )

        results = extract_csv(sources)

        context = get_current_context()
        run_id = str(context.get("run_id", "manual"))
        safe_run_id = _safe_identifier(run_id)

        stage_dir = TMP_STAGE_ROOT / safe_run_id
        stage_dir.mkdir(parents=True, exist_ok=True)

        stage_sources: dict[str, str] = {}
        extracted_rows: dict[str, int] = {}
        for name, df in results.items():
            out_path = stage_dir / f"{name}.csv"
            df.to_csv(out_path, index=False)
            stage_sources[name] = str(out_path)
            extracted_rows[name] = int(df.shape[0])

        return {
            "raw_sources": {name: str(path) for name, path in sources.items()},
            "stage_sources": stage_sources,
            "extracted_rows": extracted_rows,
            "stage_dir": str(stage_dir),
            "fetch_weather": fetch_weather,
            "stage": _stage_report(stage_started_at, stage_start_perf),
        }

    @task(task_id="transform")
    def transform_task(extract_meta: dict[str, Any]) -> dict[str, Any]:
        import pandas as pd

        from etl.transform import profiling, transform
        from etl.validations import enforce_quality_report, post_clean_checks

        stage_started_at = utc_now_iso()
        stage_start_perf = time.perf_counter()

        stage_sources_meta = extract_meta.get("stage_sources")
        if not isinstance(stage_sources_meta, dict) or not stage_sources_meta:
            raise ValueError("extract task did not provide valid stage source metadata")

        results = {
            name: pd.read_csv(Path(path)) for name, path in stage_sources_meta.items()
        }
        profiling(results, detail=False)
        cleaned, rejected = transform(results, run_checks=False, save_outputs=True)
        validation_report = post_clean_checks(cleaned, rejected, fail_on_critical=False)
        task_logger = logging.getLogger("airflow.task")
        task_logger.info(
            "[QUALITY] report=%s", json.dumps(validation_report, sort_keys=True)
        )
        enforce_quality_report(validation_report)
        return {
            "input_rows": {name: int(df.shape[0]) for name, df in results.items()},
            "cleaned_rows": {name: int(df.shape[0]) for name, df in cleaned.items()},
            "rejected_rows": {name: int(df.shape[0]) for name, df in rejected.items()},
            "validation_report": validation_report,
            "stage": _stage_report(stage_started_at, stage_start_perf),
        }

    @task(task_id="load")
    def load_task(transform_meta: dict[str, Any]) -> dict[str, Any]:
        from etl.load import build_analytics_model, mart_lineage_counts

        stage_started_at = utc_now_iso()
        stage_start_perf = time.perf_counter()

        if not isinstance(transform_meta, dict):
            raise ValueError("transform task metadata is missing")

        mart_df = build_analytics_model()
        mart_counts = mart_lineage_counts(mart_df)
        return {
            "mart_rows": mart_counts["mart"]["rows"],
            "mart_cols": mart_counts["mart"]["cols"],
            "mart_preview_rows": mart_counts["mart_preview"]["rows"],
            "mart_preview_cols": mart_counts["mart_preview"]["cols"],
            "stage": _stage_report(stage_started_at, stage_start_perf),
        }

    @task(task_id="cleanup", trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_task(
        extract_meta: dict[str, Any] | None,
        transform_meta: dict[str, Any] | None,
        load_meta: dict[str, Any] | None,
    ) -> dict[str, Any]:
        from airflow.operators.python import get_current_context

        from etl.lineage import (
            assemble_lineage_report,
            build_observability_summary,
            collect_artifact_fingerprints,
            write_lineage_report,
        )
        from etl.load import get_output_artifact_paths

        cleanup_started_at = utc_now_iso()
        cleanup_start_perf = time.perf_counter()
        context = get_current_context()
        task_logger = logging.getLogger("airflow.task")

        extract_payload = extract_meta if isinstance(extract_meta, dict) else {}
        transform_payload = transform_meta if isinstance(transform_meta, dict) else {}
        load_payload = load_meta if isinstance(load_meta, dict) else {}

        dag_run = context.get("dag_run")
        task_states: dict[str, str | None] = {}
        if dag_run is not None:
            for task_id in ("extract", "transform", "load", "cleanup"):
                ti = dag_run.get_task_instance(task_id)
                task_states[task_id] = getattr(ti, "state", None)

        raw_sources = extract_payload.get("raw_sources", {})
        stage_sources = extract_payload.get("stage_sources", {})
        # Hash staging CSVs before rmtree; after delete the paths still exist
        # in metadata but the files do not, so fingerprints would be empty.
        stage_fingerprints = (
            collect_artifact_fingerprints(stage_sources)
            if isinstance(stage_sources, dict)
            else {}
        )

        stage_dir_value = extract_payload.get("stage_dir")
        cleanup_status = "skipped"
        cleanup_reason = "missing_stage_dir"
        stage_dir_path: Path | None = None

        if isinstance(stage_dir_value, str) and stage_dir_value.strip():
            stage_dir_path = _resolve_stage_dir_for_cleanup(
                stage_dir_value,
                staging_root=TMP_STAGE_ROOT,
            )

            if stage_dir_path.exists():
                shutil.rmtree(stage_dir_path)
                cleanup_status = "deleted"
                cleanup_reason = "ok"
            else:
                cleanup_status = "not_found"
                cleanup_reason = "stage_dir_not_found"

        output_artifacts = get_output_artifact_paths()

        run_status = (
            "passed"
            if task_states.get("transform") == "success"
            and task_states.get("load") == "success"
            else "failed_or_partial"
        )

        lineage_report = assemble_lineage_report(
            run_id=str(context.get("run_id", "manual")),
            fetch_weather=extract_payload.get("fetch_weather"),
            status=run_status,
            started_at_utc=(
                dag_run.start_date.astimezone(timezone.utc).isoformat()
                if dag_run is not None and dag_run.start_date is not None
                else cleanup_started_at
            ),
            ended_at_utc=utc_now_iso(),
            stages={
                "extract": extract_payload.get("stage"),
                "transform": transform_payload.get("stage"),
                "load": load_payload.get("stage"),
                "cleanup": _stage_report(cleanup_started_at, cleanup_start_perf),
            },
            extract_rows=extract_payload.get("extracted_rows"),
            cleaned_rows=transform_payload.get("cleaned_rows"),
            rejected_rows=transform_payload.get("rejected_rows"),
            mart=(
                {
                    "rows": load_payload["mart_rows"],
                    "cols": load_payload["mart_cols"],
                }
                if "mart_rows" in load_payload
                else None
            ),
            mart_preview=(
                {
                    "rows": load_payload["mart_preview_rows"],
                    "cols": load_payload["mart_preview_cols"],
                }
                if "mart_preview_rows" in load_payload
                else None
            ),
            artifacts={
                "raw_sources": (
                    collect_artifact_fingerprints(raw_sources)
                    if isinstance(raw_sources, dict)
                    else {}
                ),
                "stage_sources": stage_fingerprints,
                "final_outputs": collect_artifact_fingerprints(output_artifacts),
            },
            quality_report=transform_payload.get("validation_report"),
            orchestration="airflow",
            extra={
                "dag_id": str(context.get("dag").dag_id),
                "task_states": task_states,
                "cleanup": {
                    "status": cleanup_status,
                    "reason": cleanup_reason,
                    "stage_dir": (
                        str(stage_dir_path) if stage_dir_path is not None else None
                    ),
                },
            },
        )
        lineage_output = write_lineage_report(
            lineage_report,
            output_dir=PROJECT_ROOT / "logs" / "etl" / "lineage",
        )
        summary = build_observability_summary(lineage_report)
        summary["lineage_path"] = str(lineage_output)
        task_logger.info(
            "[OBSERVABILITY] summary=%s", json.dumps(summary, sort_keys=True)
        )

        return {
            "cleanup_status": cleanup_status,
            "reason": cleanup_reason,
            "stage_dir": str(stage_dir_path) if stage_dir_path is not None else None,
            "lineage_path": str(lineage_output),
        }

    extract_meta = extract_task()
    transform_meta = transform_task(extract_meta)
    load_meta = load_task(transform_meta)
    cleanup_task(extract_meta, transform_meta, load_meta)


retail_weather_etl_dag()
