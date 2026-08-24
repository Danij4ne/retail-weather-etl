from __future__ import annotations

import logging

import pytest

import etl.alerts as alerts_module


class _DummyTaskInstance:
    def __init__(self, dag_id: str, task_id: str, try_number: int):
        self.dag_id = dag_id
        self.task_id = task_id
        self.try_number = try_number


class _DummyDagRun:
    def __init__(self, run_id: str):
        self.run_id = run_id


def test_send_alert_returns_false_when_disabled(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("ALERTS_ENABLED", raising=False)

    called = {"n": 0}

    def _fake_post(*args, **kwargs):
        called["n"] += 1
        raise AssertionError("requests.post should not be called")

    monkeypatch.setattr(alerts_module.requests, "post", _fake_post)

    assert (
        alerts_module.send_alert(
            title="Pipeline failed",
            message="Something broke.",
        )
        is False
    )
    assert called["n"] == 0


def test_send_alert_warns_when_slack_webhook_is_missing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    monkeypatch.setenv("ALERTS_ENABLED", "true")
    monkeypatch.setenv("ALERTS_CHANNEL", "slack")
    monkeypatch.delenv("ALERTS_SLACK_WEBHOOK_URL", raising=False)

    with caplog.at_level(logging.WARNING):
        sent = alerts_module.send_alert(
            title="Pipeline failed",
            message="Something broke.",
        )

    assert sent is False
    assert "ALERTS_SLACK_WEBHOOK_URL is missing" in caplog.text


def test_send_alert_posts_to_slack_webhook(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ALERTS_ENABLED", "true")
    monkeypatch.setenv("ALERTS_CHANNEL", "slack")
    monkeypatch.setenv("ALERTS_PROJECT", "retail-weather-etl")
    monkeypatch.setenv("ALERTS_ENVIRONMENT", "dev")
    monkeypatch.setenv("ALERTS_TIMEOUT_SECONDS", "7.5")
    monkeypatch.setenv(
        "ALERTS_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/test"
    )

    captured: dict[str, object] = {}

    class _Response:
        def raise_for_status(self) -> None:
            return None

    def _fake_post(url, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return _Response()

    monkeypatch.setattr(alerts_module.requests, "post", _fake_post)

    sent = alerts_module.send_alert(
        title="Quality gate failed",
        message="Critical validation checks failed.",
        severity="high",
        context={"run_id": "run-123", "stage": "transform"},
    )

    assert sent is True
    assert captured["url"] == "https://hooks.slack.com/services/test"
    assert captured["timeout"] == 7.5
    payload = captured["json"]
    assert isinstance(payload, dict)
    assert "Quality gate failed" in str(payload["text"])
    assert "severity: HIGH" in str(payload["text"])
    assert "- run_id: run-123" in str(payload["text"])


def test_send_alert_falls_back_for_blank_project_env_and_invalid_severity(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ALERTS_ENABLED", "true")
    monkeypatch.setenv("ALERTS_CHANNEL", "slack")
    monkeypatch.setenv("ALERTS_PROJECT", "   ")
    monkeypatch.setenv("ALERTS_ENVIRONMENT", "   ")
    monkeypatch.setenv("ALERTS_TIMEOUT_SECONDS", "not-a-number")
    monkeypatch.setenv(
        "ALERTS_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/test"
    )

    captured: dict[str, object] = {}

    class _Response:
        def raise_for_status(self) -> None:
            return None

    def _fake_post(url, json=None, timeout=None):
        captured["json"] = json
        captured["timeout"] = timeout
        return _Response()

    monkeypatch.setattr(alerts_module.requests, "post", _fake_post)

    sent = alerts_module.send_alert(
        title="Pipeline failed",
        message="Unhandled exception.",
        severity="urgent",
    )

    assert sent is True
    assert captured["timeout"] == 5.0
    payload = captured["json"]
    assert isinstance(payload, dict)
    assert "[retail-weather-etl] Pipeline failed" in str(payload["text"])
    assert "severity: HIGH" in str(payload["text"])
    assert "environment: local" in str(payload["text"])


def test_send_alert_does_not_mask_delivery_failures(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    monkeypatch.setenv("ALERTS_ENABLED", "true")
    monkeypatch.setenv("ALERTS_CHANNEL", "slack")
    monkeypatch.setenv(
        "ALERTS_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/test"
    )

    def _boom(*args, **kwargs):
        raise RuntimeError("slack down")

    monkeypatch.setattr(alerts_module.requests, "post", _boom)

    with caplog.at_level(logging.ERROR):
        sent = alerts_module.send_alert(
            title="Pipeline failed",
            message="Unhandled exception.",
        )

    assert sent is False
    assert "[ALERT] failed to send alert" in caplog.text


def test_airflow_failure_callback_reuses_send_alert(
    monkeypatch: pytest.MonkeyPatch,
):
    captured: dict[str, object] = {}

    def _fake_send_alert(**kwargs: object) -> bool:
        captured.update(kwargs)
        return True

    monkeypatch.setattr(alerts_module, "send_alert", _fake_send_alert)

    context = {
        "task_instance": _DummyTaskInstance(
            dag_id="retail_weather_etl",
            task_id="transform",
            try_number=2,
        ),
        "dag_run": _DummyDagRun(run_id="scheduled__2026-05-21"),
        "exception": RuntimeError("quality gate exploded"),
    }

    alerts_module.airflow_failure_callback(context)

    assert captured["title"] == "Airflow task failed"
    assert captured["severity"] == "high"
    callback_context = captured["context"]
    assert isinstance(callback_context, dict)
    assert callback_context["dag_id"] == "retail_weather_etl"
    assert callback_context["task_id"] == "transform"
    assert callback_context["run_id"] == "scheduled__2026-05-21"
    assert callback_context["try_number"] == 2
    assert callback_context["error_type"] == "RuntimeError"
    assert callback_context["error_message"] == "quality gate exploded"
