from __future__ import annotations

import logging
from typing import Any

from etl.settings import DEFAULT_SETTINGS, get_settings

logger = logging.getLogger("etl.transform")


def _sanitize_normalization_map(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}

    normalized: dict[str, str] = {}
    for key, value in raw.items():
        if key is None or value is None:
            continue
        key_s = str(key).strip().lower()
        value_s = str(value).strip().lower()
        if key_s and value_s:
            normalized[key_s] = value_s
    return normalized


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return default


def get_normalization_config() -> (
    tuple[dict[str, str], dict[str, str], bool, dict[str, str]]
):
    settings = get_settings()
    normalization_cfg = (
        settings.get("normalization", {}) if isinstance(settings, dict) else {}
    )
    defaults_cfg = DEFAULT_SETTINGS.get("normalization", {})

    default_city_map = _sanitize_normalization_map(defaults_cfg.get("city_map", {}))
    default_category_map = _sanitize_normalization_map(
        defaults_cfg.get("category_map", {})
    )
    default_product_name_cfg = (
        defaults_cfg.get("product_name", {}) if isinstance(defaults_cfg, dict) else {}
    )
    default_leet_enabled = _coerce_bool(
        default_product_name_cfg.get("leet_enabled", True), default=True
    )
    default_leet_map = _sanitize_normalization_map(
        default_product_name_cfg.get("leet_map", {})
    )

    city_map = _sanitize_normalization_map(normalization_cfg.get("city_map", {}))
    category_map = _sanitize_normalization_map(
        normalization_cfg.get("category_map", {})
    )
    product_name_cfg = (
        normalization_cfg.get("product_name", {})
        if isinstance(normalization_cfg, dict)
        else {}
    )
    leet_enabled = _coerce_bool(
        product_name_cfg.get("leet_enabled", default_leet_enabled),
        default=default_leet_enabled,
    )
    leet_map = _sanitize_normalization_map(product_name_cfg.get("leet_map", {}))

    if not city_map:
        logger.warning(
            "[TRANSFORM][normalization] city_map missing/empty in config; using defaults size=%s",
            len(default_city_map),
        )
        city_map = default_city_map
    if not category_map:
        logger.warning(
            "[TRANSFORM][normalization] category_map missing/empty in config; using defaults size=%s",
            len(default_category_map),
        )
        category_map = default_category_map
    if not leet_map:
        logger.warning(
            "[TRANSFORM][normalization] leet_map missing/empty in config; using defaults size=%s",
            len(default_leet_map),
        )
        leet_map = default_leet_map

    logger.debug(
        "[TRANSFORM][normalization] resolved config city_map=%s category_map=%s leet_enabled=%s leet_map=%s",
        len(city_map),
        len(category_map),
        leet_enabled,
        len(leet_map),
    )

    return city_map, category_map, leet_enabled, leet_map
