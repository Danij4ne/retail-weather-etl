from __future__ import annotations

from etl.lineage import assemble_lineage_report, build_observability_summary


def test_build_observability_summary_extracts_operational_view():
    summary = build_observability_summary(
        {
            "run_id": "run-123",
            "pipeline": "retail-weather-etl",
            "orchestration": "airflow",
            "status": "failed_or_partial",
            "started_at_utc": "2026-05-21T10:00:00+00:00",
            "ended_at_utc": "2026-05-21T10:01:00+00:00",
            "duration_seconds": 60.0,
            "stages": {
                "extract": {"duration_seconds": 1.2},
                "transform": {"duration_seconds": 2.5},
                "load": {"duration_seconds": 0.7},
            },
            "row_counts": {
                "extract": {"sales": 100},
                "cleaned": {"sales": 90},
                "rejected": {"sales": 10},
                "mart": {"rows": 12, "cols": 8},
            },
            "quality_report": {
                "status": "failed",
                "warnings": ["high reject rate"],
                "critical_failures": ["duplicated sale_id"],
            },
            "artifacts": {
                "final_outputs": {
                    "gold_mart_parquet": {"exists": True},
                    "gold_dim_customers_parquet": {"exists": False},
                }
            },
            "error": {"type": "QualityGateError", "message": "blocked"},
        }
    )

    assert summary["run_id"] == "run-123"
    assert summary["orchestration"] == "airflow"
    assert summary["status"] == "failed_or_partial"
    assert summary["stages"] == {"extract": 1.2, "transform": 2.5, "load": 0.7}
    assert summary["rows"]["extract"] == {"sales": 100}
    assert summary["rows"]["mart"] == {"rows": 12, "cols": 8}
    assert summary["quality"]["warnings_count"] == 1
    assert summary["quality"]["critical_failures_count"] == 1
    assert summary["artifacts"]["final_outputs_total"] == 2
    assert summary["artifacts"]["final_outputs_missing"] == 1
    assert summary["error"] == {"type": "QualityGateError", "message": "blocked"}


def test_build_observability_summary_prefers_full_mart_over_preview():
    summary = build_observability_summary(
        {
            "row_counts": {
                "mart": {"rows": 415, "cols": 14},
                "mart_preview": {"rows": 10, "cols": 14},
            }
        }
    )

    assert summary["rows"]["mart"] == {"rows": 415, "cols": 14}


def test_assemble_lineage_report_owns_unpublished_rows_and_airflow_extras():
    report = assemble_lineage_report(
        run_id="run-456",
        fetch_weather=False,
        status="passed",
        started_at_utc="2026-05-21T10:00:00+00:00",
        ended_at_utc="2026-05-21T10:01:00+00:00",
        stages={"extract": {"duration_seconds": 1.2}},
        extract_rows={"sales": 100, "customers": 10},
        cleaned_rows={"sales": 90, "customers": 8},
        rejected_rows={"sales": 7},
        mart_preview={"rows": 10, "cols": 14},
        mart={"rows": 415, "cols": 14},
        artifacts={"final_outputs": {"gold_mart_parquet": {"exists": True}}},
        quality_report={"status": "passed", "warnings": [], "critical_failures": []},
        orchestration="airflow",
        extra={"dag_id": "retail_weather_etl", "cleanup": {"status": "deleted"}},
    )

    assert report["pipeline"] == "retail-weather-etl"
    assert report["orchestration"] == "airflow"
    assert report["dag_id"] == "retail_weather_etl"
    assert report["cleanup"] == {"status": "deleted"}
    assert report["row_counts"]["unpublished"] == {"sales": 3, "customers": 2}
    assert report["row_counts"]["mart"] == {"rows": 415, "cols": 14}
    assert report["row_counts"]["mart_preview"] == {"rows": 10, "cols": 14}
    assert report["quality_report"]["status"] == "passed"
