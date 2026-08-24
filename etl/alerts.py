from __future__ import annotations

import logging
import os
from typing import Any, Mapping

import requests

logger = logging.getLogger("etl.alerts")
_VALID_SEVERITIES = {"low", "medium", "high", "critical"}


def _alerts_enabled() -> bool:
    raw = os.getenv("ALERTS_ENABLED", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _build_alert_text(
    title: str,
    message: str,
    severity: str,
    context: Mapping[str, Any] | None = None,
) -> str:
    project = os.getenv("ALERTS_PROJECT", "retail-weather-etl").strip() or (
        "retail-weather-etl"
    )
    environment = os.getenv("ALERTS_ENVIRONMENT", "local").strip() or "local"
    normalized_severity = severity.lower()
    if normalized_severity not in _VALID_SEVERITIES:
        normalized_severity = "high"

    lines = [
        f"[{project}] {title}",
        f"severity: {normalized_severity.upper()}",
        f"environment: {environment}",
        message,
    ]

    context_lines: list[str] = []
    for key, value in (context or {}).items():
        if value is None:
            continue
        context_lines.append(f"- {key}: {value}")

    if context_lines:
        lines.extend(["", "context:"])
        lines.extend(context_lines)

    return "\n".join(lines)


def _send_slack_webhook(text: str) -> bool:
    webhook_url = os.getenv("ALERTS_SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        logger.warning(
            "[ALERT] ALERTS_CHANNEL=slack but ALERTS_SLACK_WEBHOOK_URL is missing"
        )
        return False

    try:
        timeout = float(os.getenv("ALERTS_TIMEOUT_SECONDS", "5"))
    except ValueError:
        timeout = 5.0

    response = requests.post(webhook_url, json={"text": text}, timeout=timeout)
    response.raise_for_status()
    return True


def _safe_attr(obj: Any, name: str) -> Any:
    return getattr(obj, name, None) if obj is not None else None


def send_alert(
    title: str,
    message: str,
    severity: str = "high",
    context: Mapping[str, Any] | None = None,
) -> bool:
    """Send an optional runtime alert without masking the main pipeline error."""

    if not _alerts_enabled():
        return False

    channel = os.getenv("ALERTS_CHANNEL", "none").strip().lower() or "none"
    if channel == "none":
        return False

    payload_text = _build_alert_text(title, message, severity, context)

    try:
        if channel == "slack":
            return _send_slack_webhook(payload_text)

        logger.warning("[ALERT] unsupported ALERTS_CHANNEL=%s", channel)
        return False
    except Exception:
        logger.exception("[ALERT] failed to send alert")
        return False


def airflow_failure_callback(context: Mapping[str, Any]) -> None:
    """Send an optional alert for failed Airflow task instances."""

    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    send_alert(
        title="Airflow task failed",
        message="A retail-weather-etl Airflow task failed.",
        severity="high",
        context={
            "dag_id": _safe_attr(task_instance, "dag_id"),
            "task_id": _safe_attr(task_instance, "task_id"),
            "run_id": _safe_attr(dag_run, "run_id") or context.get("run_id"),
            "try_number": _safe_attr(task_instance, "try_number"),
            "error_type": type(exception).__name__ if exception is not None else None,
            "error_message": str(exception) if exception is not None else None,
        },
    )
