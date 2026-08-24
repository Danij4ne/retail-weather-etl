import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd
import pytest

import pipelines.pipeline as pipeline_module
from etl.validations import QualityGateError


def _raw_results() -> dict[str, pd.DataFrame]:
    return {
        "customers": pd.DataFrame({"customer_id": [1]}),
        "products": pd.DataFrame({"product_id": [1001]}),
        "sales": pd.DataFrame({"sale_id": [10]}),
        "weather_daily": pd.DataFrame({"date": ["2025-01-01"]}),
    }


def _cleaned_rejected() -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    cleaned = {
        "customers": pd.DataFrame({"customer_id": [1]}),
        "products": pd.DataFrame({"product_id": [1001]}),
        "sales": pd.DataFrame({"sale_id": [10]}),
        "weather_daily": pd.DataFrame({"date": ["2025-01-01"]}),
    }
    rejected = {"sales": pd.DataFrame({"sale_id": []})}
    return cleaned, rejected


def _mock_lineage_write(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        pipeline_module,
        "write_lineage_report",
        lambda report: tmp_path / "lineage.json",
    )


def test_setup_logging_keeps_repeated_messages(tmp_path: Path):
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_filters = list(root_logger.filters)
    original_level = root_logger.level

    try:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for log_filter in list(root_logger.filters):
            root_logger.removeFilter(log_filter)

        log_file = tmp_path / "pipeline.log"
        pipeline_module.setup_logging(log_file=str(log_file), level="INFO")

        logger = logging.getLogger("etl.transform")
        logger.info("Saved %s rows", 10)
        logger.info("Saved %s rows", 10)

        for handler in root_logger.handlers:
            handler.flush()

        logged_text = log_file.read_text(encoding="utf-8")
        assert logged_text.count("Saved 10 rows") == 2
        assert root_logger.filters == []
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            handler.close()
        for log_filter in list(root_logger.filters):
            root_logger.removeFilter(log_filter)

        root_logger.setLevel(original_level)
        for handler in original_handlers:
            root_logger.addHandler(handler)
        for log_filter in original_filters:
            root_logger.addFilter(log_filter)


def test_setup_logging_uses_configured_rotation_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_filters = list(root_logger.filters)
    original_level = root_logger.level

    try:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for log_filter in list(root_logger.filters):
            root_logger.removeFilter(log_filter)

        configured_log_path = tmp_path / "custom_logs" / "etl.log"
        monkeypatch.setattr(
            pipeline_module,
            "get_settings",
            lambda: {
                "logging": {
                    "file": str(configured_log_path),
                    "max_bytes": 1234,
                    "backup_count": 7,
                    "level": "WARNING",
                }
            },
        )

        pipeline_module.setup_logging()

        file_handler = next(
            handler
            for handler in root_logger.handlers
            if getattr(handler, "name", "") == "etl_file"
        )

        assert isinstance(file_handler, RotatingFileHandler)
        assert Path(file_handler.baseFilename) == configured_log_path.resolve()
        assert file_handler.maxBytes == 1234
        assert file_handler.backupCount == 7
        assert file_handler.level == logging.WARNING
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            handler.close()
        for log_filter in list(root_logger.filters):
            root_logger.removeFilter(log_filter)

        root_logger.setLevel(original_level)
        for handler in original_handlers:
            root_logger.addHandler(handler)
        for log_filter in original_filters:
            root_logger.addFilter(log_filter)


def test_build_lineage_skeleton_returns_expected_base_shape():
    lineage = pipeline_module._build_lineage_skeleton(
        "20260521T000000000000Z", fetch_weather=False
    )

    assert lineage["run_id"] == "20260521T000000000000Z"
    assert lineage["pipeline"] == "retail-weather-etl"
    assert lineage["fetch_weather"] is False
    assert lineage["status"] == "running"
    assert lineage["stages"] == {}
    assert lineage["row_counts"] == {}
    assert lineage["artifacts"] == {}
    assert isinstance(lineage["started_at_utc"], str)


def test_run_pipeline_calls_weather_api_when_fetch_weather_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    weather_called = {"n": 0}
    _mock_lineage_write(monkeypatch, tmp_path)

    monkeypatch.setattr(pipeline_module, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(
        pipeline_module,
        "get_sources",
        lambda: {
            "customers": tmp_path / "customers.csv",
            "products": tmp_path / "products.csv",
            "sales": tmp_path / "sales.csv",
            "weather_daily": tmp_path / "weather_daily.csv",
        },
    )
    monkeypatch.setattr(pipeline_module, "extract_csv", lambda sources: _raw_results())
    monkeypatch.setattr(pipeline_module, "profiling", lambda *a, **k: None)
    monkeypatch.setattr(
        pipeline_module, "transform", lambda *a, **k: _cleaned_rejected()
    )
    monkeypatch.setattr(
        pipeline_module,
        "post_clean_checks",
        lambda *a, **k: {"status": "passed", "critical_failures": []},
    )
    monkeypatch.setattr(
        pipeline_module,
        "build_analytics_model",
        lambda: pd.DataFrame({"sale_date": ["2025-01-01"], "total_units": [1]}),
    )

    def _fake_extract_weather_api():
        weather_called["n"] += 1

    monkeypatch.setattr(
        pipeline_module, "extract_weather_api", _fake_extract_weather_api
    )

    result = pipeline_module.run_pipeline(fetch_weather=True)

    assert weather_called["n"] == 1
    assert isinstance(result, pd.DataFrame)
    assert not result.empty


def test_run_pipeline_logs_observability_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    _mock_lineage_write(monkeypatch, tmp_path)
    monkeypatch.setattr(pipeline_module, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(
        pipeline_module,
        "get_sources",
        lambda: {
            "customers": tmp_path / "customers.csv",
            "products": tmp_path / "products.csv",
            "sales": tmp_path / "sales.csv",
            "weather_daily": tmp_path / "weather_daily.csv",
        },
    )
    monkeypatch.setattr(pipeline_module, "extract_csv", lambda sources: _raw_results())
    monkeypatch.setattr(pipeline_module, "profiling", lambda *a, **k: None)
    monkeypatch.setattr(
        pipeline_module, "transform", lambda *a, **k: _cleaned_rejected()
    )
    monkeypatch.setattr(
        pipeline_module,
        "post_clean_checks",
        lambda *a, **k: {"status": "passed", "critical_failures": [], "warnings": []},
    )

    def _fake_mart():
        preview = pd.DataFrame({"sale_date": ["2025-01-01"], "total_units": [1]})
        preview.attrs["mart_rows"] = 415
        preview.attrs["mart_cols"] = 14
        return preview

    monkeypatch.setattr(pipeline_module, "build_analytics_model", _fake_mart)
    monkeypatch.setattr(pipeline_module, "extract_weather_api", lambda: None)

    with caplog.at_level(logging.INFO):
        pipeline_module.run_pipeline(fetch_weather=True)

    assert "[OBSERVABILITY] summary=" in caplog.text
    assert '"mart": {"cols": 14, "rows": 415}' in caplog.text
    assert "lineage.json" in caplog.text


def test_run_pipeline_offline_raises_if_weather_file_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _mock_lineage_write(monkeypatch, tmp_path)
    monkeypatch.setattr(pipeline_module, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(
        pipeline_module,
        "get_sources",
        lambda: {"weather_daily": tmp_path / "missing.csv"},
    )

    with pytest.raises(
        FileNotFoundError, match="fetch_weather=False but file not found"
    ):
        pipeline_module.run_pipeline(fetch_weather=False)


def test_run_pipeline_raises_quality_gate_error_when_validation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _mock_lineage_write(monkeypatch, tmp_path)
    weather_csv = tmp_path / "weather_daily.csv"
    weather_csv.write_text("date\n2025-01-01\n", encoding="utf-8")

    build_called = {"n": 0}
    alert_calls: list[dict[str, object]] = []

    def _fake_send_alert(**kwargs: object) -> bool:
        alert_calls.append(dict(kwargs))
        return True

    monkeypatch.setattr(pipeline_module, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(pipeline_module, "send_alert", _fake_send_alert)
    monkeypatch.setattr(
        pipeline_module,
        "get_sources",
        lambda: {
            "customers": tmp_path / "customers.csv",
            "products": tmp_path / "products.csv",
            "sales": tmp_path / "sales.csv",
            "weather_daily": weather_csv,
        },
    )
    monkeypatch.setattr(pipeline_module, "extract_csv", lambda sources: _raw_results())
    monkeypatch.setattr(pipeline_module, "profiling", lambda *a, **k: None)
    monkeypatch.setattr(
        pipeline_module, "transform", lambda *a, **k: _cleaned_rejected()
    )
    monkeypatch.setattr(
        pipeline_module,
        "post_clean_checks",
        lambda *a, **k: {
            "status": "failed",
            "critical_failures": ["duplicated sale_id"],
            "warnings": [],
            "metrics": {},
        },
    )

    def _fake_build_analytics_model():
        build_called["n"] += 1
        return pd.DataFrame()

    monkeypatch.setattr(
        pipeline_module, "build_analytics_model", _fake_build_analytics_model
    )

    with pytest.raises(QualityGateError, match="quality gate failed"):
        pipeline_module.run_pipeline(fetch_weather=False)

    assert build_called["n"] == 0
    assert len(alert_calls) == 1
    assert alert_calls[0]["title"] == "Quality gate failed"
    assert alert_calls[0]["severity"] == "high"
    alert_message = str(alert_calls[0]["message"])
    assert "stopped before load" not in alert_message.lower()
    assert "silver" in alert_message.lower()
    assert "gold" in alert_message.lower()


def test_run_pipeline_sends_alert_on_unhandled_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _mock_lineage_write(monkeypatch, tmp_path)
    weather_csv = tmp_path / "weather_daily.csv"
    weather_csv.write_text("date\n2025-01-01\n", encoding="utf-8")

    alert_calls: list[dict[str, object]] = []

    def _fake_send_alert(**kwargs: object) -> bool:
        alert_calls.append(dict(kwargs))
        return True

    monkeypatch.setattr(pipeline_module, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(pipeline_module, "send_alert", _fake_send_alert)
    monkeypatch.setattr(
        pipeline_module,
        "get_sources",
        lambda: {
            "customers": tmp_path / "customers.csv",
            "products": tmp_path / "products.csv",
            "sales": tmp_path / "sales.csv",
            "weather_daily": weather_csv,
        },
    )
    monkeypatch.setattr(pipeline_module, "extract_csv", lambda sources: _raw_results())
    monkeypatch.setattr(pipeline_module, "profiling", lambda *a, **k: None)
    monkeypatch.setattr(
        pipeline_module, "transform", lambda *a, **k: _cleaned_rejected()
    )
    monkeypatch.setattr(
        pipeline_module,
        "post_clean_checks",
        lambda *a, **k: {"status": "passed", "critical_failures": []},
    )

    def _boom():
        raise RuntimeError("duckdb exploded")

    monkeypatch.setattr(pipeline_module, "build_analytics_model", _boom)

    with pytest.raises(RuntimeError, match="duckdb exploded"):
        pipeline_module.run_pipeline(fetch_weather=False)

    assert len(alert_calls) == 1
    assert alert_calls[0]["title"] == "Pipeline failed"
    assert alert_calls[0]["severity"] == "high"
