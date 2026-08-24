from __future__ import annotations

import logging
import os
from copy import deepcopy
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

logger = logging.getLogger("etl.settings")

PATH_KEYS = (
    "raw_dir",
    "silver_dir",
    "rejected_dir",
    "gold_csv_dir",
    "gold_parquet_dir",
    "sql_model",
    "duckdb_path",
)

SUPPORTED_EXPORT_FORMATS = {"csv", "parquet"}


# Base fallback configuration used when YAML is missing or incomplete.
DEFAULT_SETTINGS: dict[str, Any] = {
    "project": {
        "name": "retail-weather-etl",
    },
    "paths": {
        "raw_dir": "data/raw",
        "silver_dir": "data/processed/silver",
        "rejected_dir": "data/processed/rejected",
        "gold_csv_dir": "data/processed/gold/csv",
        "gold_parquet_dir": "data/processed/gold/parquet",
        "sql_model": "sql/build_analytics_model.sql",
        "duckdb_path": "warehouse.duckdb",
    },
    "logging": {
        "file": "logs/etl/pipeline.log",
        "max_bytes": 2_000_000,
        "backup_count": 5,
        "level": "INFO",
    },
    "exports": {
        "mart_formats": ["parquet"],
        "star_schema_formats": ["parquet"],
    },
    "weather": {
        "provider": "open-meteo-archive",
        "timezone": "Europe/Madrid",
        "start_date": "2025-01-01",
        "end_date": "2025-03-31",
        "request": {
            "timeout_seconds": 30,
            "max_retries": 3,
            "backoff_base_seconds": 1.0,
            "jitter_max_seconds": 0.5,
            "max_workers": 4,
        },
        "cities": {
            "Madrid": {"lat": 40.4168, "lon": -3.7038},
            "Barcelona": {"lat": 41.3851, "lon": 2.1734},
        },
    },
    "normalization": {
        "city_map": {
            "madrid": "madrid",
            "madríd": "madrid",
            "madird": "madrid",
            "mad rid": "madrid",
            "barcelona": "barcelona",
            "bar celona": "barcelona",
            "barcel0na": "barcelona",
            "barcelóna": "barcelona",
        },
        "category_map": {
            "sportwear": "sportswear",
            "sports wear": "sportswear",
            "sportswear": "sportswear",
            " sportswear": "sportswear",
            "accesories": "accessories",
            "accessories": "accessories",
            "access0ries": "accessories",
            "fotwear": "footwear",
            "footw3ar": "footwear",
            "f00twear": "footwear",
            "footwear": "footwear",
            "outdoorgear": "outdoor gear",
            "outdor gear": "outdoor gear",
            "outdoor gear": "outdoor gear",
            "hydrtion": "hydration",
            "hydration": "hydration",
            "misc": "unknown",
            "other": "unknown",
            "others": "unknown",
        },
        "product_name": {
            "leet_enabled": True,
            "leet_map": {
                "0": "o",
                "1": "i",
                "3": "e",
                "4": "a",
                "5": "s",
                "7": "t",
            },
        },
    },
    "quality": {
        # Warning floors calibrated to the synthetic source baseline.
        "max_reject_rate": 0.45,
        "min_valid_price_coverage": 0.90,
        "max_unknown_city_rate": 0.08,
    },
    "analytics": {
        "buckets": {
            "rain": {
                "light_rain_lt_mm": 2.0,
            },
            "temperature": {
                "cold_lt_c": 10.0,
                "mild_lte_c": 25.0,
            },
        },
    },
}


class _SettingsBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _ProjectSettings(_SettingsBaseModel):
    name: str = "retail-weather-etl"


class _PathsSettings(_SettingsBaseModel):
    raw_dir: str = "data/raw"
    silver_dir: str = "data/processed/silver"
    rejected_dir: str = "data/processed/rejected"
    gold_csv_dir: str = "data/processed/gold/csv"
    gold_parquet_dir: str = "data/processed/gold/parquet"
    sql_model: str = "sql/build_analytics_model.sql"
    duckdb_path: str = "warehouse.duckdb"


class _LoggingSettings(_SettingsBaseModel):
    file: str = "logs/etl/pipeline.log"
    max_bytes: int = Field(default=2_000_000, gt=0)
    backup_count: int = Field(default=5, ge=0)
    level: str = "INFO"


class _ExportsSettings(_SettingsBaseModel):
    mart_formats: list[str] = Field(default_factory=lambda: ["parquet"])
    star_schema_formats: list[str] = Field(default_factory=lambda: ["parquet"])

    @field_validator("mart_formats", "star_schema_formats")
    @classmethod
    def _validate_export_formats(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("must contain at least one format")

        normalized: list[str] = []
        for raw_format in value:
            export_format = str(raw_format).strip().lower()
            if export_format not in SUPPORTED_EXPORT_FORMATS:
                raise ValueError(f"unsupported format: {export_format}")
            if export_format not in normalized:
                normalized.append(export_format)
        return normalized


class _WeatherRequestSettings(_SettingsBaseModel):
    timeout_seconds: int = Field(default=30, gt=0)
    max_retries: int = Field(default=3, ge=0)
    backoff_base_seconds: float = Field(default=1.0, ge=0)
    jitter_max_seconds: float = Field(default=0.5, ge=0)
    max_workers: int = Field(default=4, ge=1)


class _WeatherCitySettings(_SettingsBaseModel):
    lat: float
    lon: float


class _WeatherSettings(_SettingsBaseModel):
    provider: str = "open-meteo-archive"
    timezone: str = "Europe/Madrid"
    start_date: date = date(2025, 1, 1)
    end_date: date = date(2025, 3, 31)
    request: _WeatherRequestSettings = Field(default_factory=_WeatherRequestSettings)
    cities: dict[str, _WeatherCitySettings] = Field(
        default_factory=lambda: {
            "Madrid": _WeatherCitySettings(lat=40.4168, lon=-3.7038),
            "Barcelona": _WeatherCitySettings(lat=41.3851, lon=2.1734),
        }
    )

    @model_validator(mode="after")
    def _validate_date_range(self) -> "_WeatherSettings":
        if self.start_date > self.end_date:
            raise ValueError("weather.start_date must be <= weather.end_date")
        return self


class _NormalizationProductNameSettings(_SettingsBaseModel):
    leet_enabled: bool = True
    leet_map: dict[str, str] = Field(
        default_factory=lambda: {
            "0": "o",
            "1": "i",
            "3": "e",
            "4": "a",
            "5": "s",
            "7": "t",
        }
    )


class _NormalizationSettings(_SettingsBaseModel):
    city_map: dict[str, str] = Field(
        default_factory=lambda: deepcopy(DEFAULT_SETTINGS["normalization"]["city_map"])
    )
    category_map: dict[str, str] = Field(
        default_factory=lambda: deepcopy(
            DEFAULT_SETTINGS["normalization"]["category_map"]
        )
    )
    product_name: _NormalizationProductNameSettings = Field(
        default_factory=_NormalizationProductNameSettings
    )


class _QualitySettings(_SettingsBaseModel):
    max_reject_rate: float = Field(default=0.45, ge=0.0, le=1.0)
    min_valid_price_coverage: float = Field(default=0.90, ge=0.0, le=1.0)
    max_unknown_city_rate: float = Field(default=0.08, ge=0.0, le=1.0)


class _AnalyticsRainBucketsSettings(_SettingsBaseModel):
    light_rain_lt_mm: float = Field(default=2.0, ge=0.0)


class _AnalyticsTemperatureBucketsSettings(_SettingsBaseModel):
    cold_lt_c: float = 10.0
    mild_lte_c: float = 25.0

    @model_validator(mode="after")
    def _validate_temperature_range(self) -> "_AnalyticsTemperatureBucketsSettings":
        if self.cold_lt_c > self.mild_lte_c:
            raise ValueError("temperature.cold_lt_c must be <= mild_lte_c")
        return self


class _AnalyticsBucketsSettings(_SettingsBaseModel):
    rain: _AnalyticsRainBucketsSettings = Field(
        default_factory=_AnalyticsRainBucketsSettings
    )
    temperature: _AnalyticsTemperatureBucketsSettings = Field(
        default_factory=_AnalyticsTemperatureBucketsSettings
    )


class _AnalyticsSettings(_SettingsBaseModel):
    buckets: _AnalyticsBucketsSettings = Field(
        default_factory=_AnalyticsBucketsSettings
    )


class _SettingsModel(_SettingsBaseModel):
    project: _ProjectSettings = Field(default_factory=_ProjectSettings)
    paths: _PathsSettings = Field(default_factory=_PathsSettings)
    logging: _LoggingSettings = Field(default_factory=_LoggingSettings)
    exports: _ExportsSettings = Field(default_factory=_ExportsSettings)
    weather: _WeatherSettings = Field(default_factory=_WeatherSettings)
    normalization: _NormalizationSettings = Field(
        default_factory=_NormalizationSettings
    )
    quality: _QualitySettings = Field(default_factory=_QualitySettings)
    analytics: _AnalyticsSettings = Field(default_factory=_AnalyticsSettings)


# Recursively merge user config on top of defaults.
def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


# Resolve config file candidates in priority order.
def _candidate_config_paths(explicit_path: str | None = None) -> list[Path]:

    candidates: list[Path] = []

    if explicit_path:
        candidates.append(Path(explicit_path))

    env_path = os.getenv("RETAIL_ETL_CONFIG")
    if env_path:
        candidates.append(Path(env_path))

    # Allow local override when running from arbitrary working directory.
    candidates.append(Path("config/config.yaml"))

    # Stable fallback to project-level config.
    project_root = Path(__file__).resolve().parents[1]
    candidates.append(project_root / "config" / "config.yaml")

    return candidates


def _normalize_relative_paths(
    settings: dict[str, Any], base_dir: Path | None
) -> dict[str, Any]:
    if base_dir is None:
        return settings

    normalized = deepcopy(settings)
    paths_cfg = normalized.get("paths")
    if not isinstance(paths_cfg, dict):
        return normalized

    for key in PATH_KEYS:
        value = paths_cfg.get(key)
        if not isinstance(value, str) or not value.strip():
            continue

        path_value = Path(value)
        if not path_value.is_absolute():
            paths_cfg[key] = str((base_dir / path_value).resolve())

    return normalized


def _validate_settings(settings: dict[str, Any]) -> dict[str, Any]:
    try:
        validated = _SettingsModel.model_validate(settings)
    except ValidationError as exc:
        logger.error("[SETTINGS] validation_error=%s", exc)
        raise ValueError(f"Invalid settings configuration: {exc}") from exc

    validated_settings: dict[str, Any] = validated.model_dump(mode="json")
    return validated_settings


# Normalize optional path-like env/config values for stable cache keys.
def _normalize_cache_key_path(path_value: str | None) -> str | None:
    if path_value is None:
        return None
    stripped = path_value.strip()
    if not stripped:
        return None
    return str(Path(stripped).expanduser().resolve())


@lru_cache(maxsize=16)
def _get_settings_cached(
    normalized_config_path: str | None,
    _normalized_env_config_path: str | None,
    normalized_base_dir_env: str | None,
) -> dict[str, Any]:
    # Key arguments are intentionally part of cache identity.
    # They make cache behavior explicit when config/env paths vary.

    base_dir = (
        Path(normalized_base_dir_env).expanduser().resolve()
        if normalized_base_dir_env
        else None
    )

    candidates = _candidate_config_paths(normalized_config_path)
    logger.debug("[SETTINGS] candidate_paths=%s", [str(p) for p in candidates])

    for path in candidates:
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
            if not isinstance(loaded, dict):
                logger.error(
                    "[SETTINGS] invalid_root_type path=%s type=%s",
                    path,
                    type(loaded).__name__,
                )
                raise ValueError(f"Config file must contain a mapping at root: {path}")

            logger.info("[SETTINGS] loaded path=%s", path)
            merged = _deep_merge(DEFAULT_SETTINGS, loaded)
            normalized = _normalize_relative_paths(merged, base_dir=base_dir)
            return _validate_settings(normalized)

    logger.warning("[SETTINGS] no config file found; using DEFAULT_SETTINGS")
    normalized_defaults = _normalize_relative_paths(
        deepcopy(DEFAULT_SETTINGS), base_dir=base_dir
    )
    return _validate_settings(normalized_defaults)


# Public API: resolve cache key context, then return an isolated copy to avoid
# accidental mutation of shared cached state across callers.
def get_settings(config_path: str | None = None) -> dict[str, Any]:
    normalized_config_path = _normalize_cache_key_path(config_path)
    normalized_env_config_path = _normalize_cache_key_path(
        os.getenv("RETAIL_ETL_CONFIG")
    )
    normalized_base_dir_env = _normalize_cache_key_path(
        os.getenv("RETAIL_ETL_BASE_DIR")
    )

    cached = _get_settings_cached(
        normalized_config_path,
        normalized_env_config_path,
        normalized_base_dir_env,
    )
    return deepcopy(cached)


def clear_settings_cache() -> None:
    """Clear the cached resolved settings, mainly for tests and config overrides."""
    _get_settings_cached.cache_clear()


# Backward compatibility for tests/callers that use get_settings.cache_clear().
get_settings.cache_clear = clear_settings_cache  # type: ignore[attr-defined]
