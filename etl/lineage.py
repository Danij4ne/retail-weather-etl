from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

logger = logging.getLogger("etl.lineage")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_path(value: str | Path) -> Path:
    if isinstance(value, Path):
        return value
    return Path(value)


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_artifact_fingerprints(
    artifacts: Mapping[str, str | Path],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name, raw_path in artifacts.items():
        path = _to_path(raw_path).resolve()
        entry: dict[str, Any] = {
            "path": path.as_posix(),
            "exists": path.exists(),
        }
        if path.exists() and path.is_file():
            entry["size_bytes"] = int(path.stat().st_size)
            entry["sha256"] = _sha256_file(path)
        result[name] = entry
    return result


def assemble_lineage_report(
    *,
    run_id: str,
    fetch_weather: bool | None,
    status: str,
    started_at_utc: str,
    ended_at_utc: str,
    stages: Mapping[str, Any] | None = None,
    extract_rows: Mapping[str, int] | None = None,
    cleaned_rows: Mapping[str, int] | None = None,
    rejected_rows: Mapping[str, int] | None = None,
    mart: Mapping[str, Any] | None = None,
    mart_preview: Mapping[str, Any] | None = None,
    artifacts: Mapping[str, Any] | None = None,
    quality_report: Mapping[str, Any] | None = None,
    error: Mapping[str, Any] | None = None,
    duration_seconds: float | None = None,
    orchestration: str = "local",
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the serializable lineage payload from stage metadata.

    Coordinators collect timings, counts, and fingerprints. This function owns
    the report shape, including unpublished-row accounting.
    """

    extract = extract_rows if isinstance(extract_rows, Mapping) else {}
    cleaned = cleaned_rows if isinstance(cleaned_rows, Mapping) else {}
    rejected = rejected_rows if isinstance(rejected_rows, Mapping) else {}

    report: dict[str, Any] = {
        "run_id": run_id,
        "pipeline": "retail-weather-etl",
        "orchestration": orchestration,
        "fetch_weather": fetch_weather,
        "status": status,
        "started_at_utc": started_at_utc,
        "ended_at_utc": ended_at_utc,
        "stages": dict(stages or {}),
        "row_counts": {
            "extract": extract_rows,
            "cleaned": cleaned_rows,
            "rejected": rejected_rows,
            "unpublished": (
                compute_unpublished_rows(extract, cleaned, rejected) if extract else {}
            ),
            "mart": mart,
            "mart_preview": mart_preview,
        },
        "quality_report": quality_report,
        "artifacts": dict(artifacts or {}),
    }
    if duration_seconds is not None:
        report["duration_seconds"] = duration_seconds
    if error is not None:
        report["error"] = error
    if extra:
        report.update(dict(extra))
    return report


def compute_unpublished_rows(
    extracted: Mapping[str, int],
    cleaned: Mapping[str, int],
    rejected: Mapping[str, int],
) -> dict[str, int]:
    """Count extracted rows that reached neither the silver nor the rejected layer.

    Deduplication discards the losing duplicates before the reject split, so those
    rows carry no `reject_reasons` and are absent from both published layers.
    Reporting the difference keeps the row accounting closed.
    """
    return {
        name: int(total) - int(cleaned.get(name, 0)) - int(rejected.get(name, 0))
        for name, total in extracted.items()
    }


def build_observability_summary(report: Mapping[str, Any]) -> dict[str, Any]:
    stages_raw = report.get("stages", {})
    stages = stages_raw if isinstance(stages_raw, Mapping) else {}

    stage_durations: dict[str, float] = {}
    for stage_name, stage_payload in stages.items():
        if not isinstance(stage_payload, Mapping):
            continue
        duration = stage_payload.get("duration_seconds")
        if isinstance(duration, int | float):
            stage_durations[str(stage_name)] = float(duration)

    row_counts_raw = report.get("row_counts", {})
    row_counts = row_counts_raw if isinstance(row_counts_raw, Mapping) else {}

    quality_raw = report.get("quality_report", {})
    quality = quality_raw if isinstance(quality_raw, Mapping) else {}

    warnings = quality.get("warnings", [])
    critical_failures = quality.get("critical_failures", [])

    artifacts_raw = report.get("artifacts", {})
    artifacts = artifacts_raw if isinstance(artifacts_raw, Mapping) else {}
    final_outputs_raw = artifacts.get("final_outputs", {})
    final_outputs = final_outputs_raw if isinstance(final_outputs_raw, Mapping) else {}

    missing_outputs = sum(
        1
        for payload in final_outputs.values()
        if isinstance(payload, Mapping) and payload.get("exists") is False
    )

    summary: dict[str, Any] = {
        "run_id": report.get("run_id"),
        "pipeline": report.get("pipeline"),
        "orchestration": report.get("orchestration", "local"),
        "status": report.get("status"),
        "started_at_utc": report.get("started_at_utc"),
        "ended_at_utc": report.get("ended_at_utc"),
        "duration_seconds": report.get("duration_seconds"),
        "stages": stage_durations,
        "rows": {
            "extract": row_counts.get("extract"),
            "cleaned": row_counts.get("cleaned"),
            "rejected": row_counts.get("rejected"),
            "unpublished": row_counts.get("unpublished"),
            "mart": row_counts.get("mart") or row_counts.get("mart_preview"),
        },
        "quality": {
            "status": quality.get("status"),
            "warnings_count": len(warnings) if isinstance(warnings, list) else 0,
            "critical_failures_count": (
                len(critical_failures) if isinstance(critical_failures, list) else 0
            ),
        },
        "artifacts": {
            "final_outputs_total": len(final_outputs),
            "final_outputs_missing": missing_outputs,
        },
        "error": report.get("error"),
    }
    return summary


def write_lineage_report(
    report: Mapping[str, Any],
    *,
    output_dir: str | Path = "logs/etl/lineage",
) -> Path:
    output_path = _to_path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    run_id_raw = str(report.get("run_id", "")).strip()
    run_id = (
        run_id_raw
        if run_id_raw
        else datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    )

    target = output_path / f"lineage_{run_id}.json"
    target.write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    logger.info("[LINEAGE] saved report path=%s", target)
    return target
