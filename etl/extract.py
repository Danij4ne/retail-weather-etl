from __future__ import annotations

import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, TypedDict, cast

import pandas as pd
import requests

from etl.settings import get_settings

logger = logging.getLogger("etl.extract")


# EXTRACT API (Weather - Open Meteo)
# We save the result AS-IS in RAW (Bronze layer)
DAILY_FIELDS = [
    "temperature_2m_mean",
    "precipitation_sum",
    "precipitation_hours",
    "weather_code",
]
DAILY_VARS = ",".join(DAILY_FIELDS)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class ExtractRuntimeConfig(TypedDict):
    base: Path
    out: Path
    cities: dict[str, tuple[float, float]]
    start_date: str
    end_date: str
    timezone: str
    request_timeout_seconds: int
    max_retries: int
    backoff_base_seconds: float
    jitter_max_seconds: float
    max_workers: int


def _build_cities(weather_cfg: dict[str, Any]) -> dict[str, tuple[float, float]]:
    """Build a validated city-to-coordinates mapping from weather settings."""
    cities_cfg = weather_cfg.get("cities", {})
    cities: dict[str, tuple[float, float]] = {}

    if not isinstance(cities_cfg, dict):
        return cities

    for key, payload in cities_cfg.items():
        if not isinstance(payload, dict):
            logger.warning(
                "[EXTRACT_API] skipping city config key=%s reason=payload_not_mapping",
                key,
            )
            continue
        if "lat" not in payload or "lon" not in payload:
            logger.warning(
                "[EXTRACT_API] skipping city config key=%s reason=missing_lat_lon",
                key,
            )
            continue
        city_name = str(payload.get("name", key)).strip().title()
        try:
            cities[city_name] = (float(payload["lat"]), float(payload["lon"]))
        except (TypeError, ValueError):
            logger.warning(
                "[EXTRACT_API] skipping city config key=%s reason=invalid_lat_lon",
                key,
            )
            continue

    return cities


def _extract_runtime_config() -> ExtractRuntimeConfig:
    """Resolve normalized runtime settings used by CSV and weather extraction."""
    settings = get_settings()
    paths = cast(dict[str, Any], settings["paths"])
    weather = cast(dict[str, Any], settings["weather"])
    request_cfg = cast(dict[str, Any], weather["request"])

    base = Path(cast(str, paths["raw_dir"]))
    out = base / "weather_daily.csv"

    cities = _build_cities(weather)
    if not cities:
        # Defensive fallback for direct calls/tests that bypass validated settings.
        cities = {
            "Madrid": (40.4168, -3.7038),
            "Barcelona": (41.3851, 2.1734),
        }

    return {
        "base": base,
        "out": out,
        "cities": cities,
        "start_date": cast(str, weather["start_date"]),
        "end_date": cast(str, weather["end_date"]),
        "timezone": cast(str, weather["timezone"]),
        "request_timeout_seconds": cast(int, request_cfg["timeout_seconds"]),
        # Number of retries after the initial attempt.
        "max_retries": cast(int, request_cfg["max_retries"]),
        "backoff_base_seconds": cast(float, request_cfg["backoff_base_seconds"]),
        "jitter_max_seconds": cast(float, request_cfg["jitter_max_seconds"]),
        "max_workers": cast(int, request_cfg["max_workers"]),
    }


def get_sources() -> dict[str, Path]:
    """Return the canonical RAW input file paths for the ETL extract stage."""
    cfg = _extract_runtime_config()
    base = cfg["base"]

    return {
        "customers": base / "customers.csv",
        "products": base / "products.csv",
        "sales": base / "sales.csv",
        "weather_daily": base / "weather_daily.csv",
    }


def ensure_raw_dir(base: Path | None = None) -> None:
    """Create the RAW directory only when an extract write is about to happen."""
    if base is None:
        base = _extract_runtime_config()["base"]
    base.mkdir(parents=True, exist_ok=True)


def _is_retryable_request_error(exc: requests.exceptions.RequestException) -> bool:
    """Classify request exceptions that should trigger retry/backoff behavior."""
    if isinstance(
        exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
    ):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        status_code = exc.response.status_code if exc.response is not None else None
        return status_code in RETRYABLE_STATUS_CODES
    return False


def _request_json_with_retry(
    *,
    city: str,
    url: str,
    params: dict[str, Any],
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    jitter_max_seconds: float,
) -> dict[str, Any]:
    """Fetch JSON with retry/backoff for transient HTTP and connection failures."""
    retries = max(0, max_retries)
    total_attempts = retries + 1

    for attempt_idx in range(total_attempts):
        attempt = attempt_idx + 1
        try:
            response = requests.get(url, params=params, timeout=timeout_seconds)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Unexpected JSON payload type (expected object)")
            return cast(dict[str, Any], payload)
        except requests.exceptions.RequestException as exc:
            retryable = _is_retryable_request_error(exc)
            is_last_attempt = attempt == total_attempts

            if not retryable or is_last_attempt:
                logger.exception(
                    "[EXTRACT_API] Request failed city=%s attempt=%s/%s retryable=%s",
                    city,
                    attempt,
                    total_attempts,
                    retryable,
                )
                raise

            backoff = backoff_base_seconds * (2**attempt_idx)
            # Operational retry jitter only; not used for secrets or tokens.
            jitter = random.uniform(0, max(0.0, jitter_max_seconds))  # nosec B311
            sleep_for = backoff + jitter

            logger.warning(
                "[EXTRACT_API] transient error city=%s attempt=%s/%s; retrying in %.2fs",
                city,
                attempt,
                total_attempts,
                sleep_for,
            )
            time.sleep(sleep_for)
        except ValueError:
            logger.exception(
                "[EXTRACT_API] Response is not valid JSON for city=%s attempt=%s/%s",
                city,
                attempt,
                total_attempts,
            )
            raise

    # Defensive fallback: logically unreachable.
    raise RuntimeError("Unreachable retry loop state")


def fetch_city_weather(
    city: str,
    lat: float,
    lon: float,
    start_date: str | None = None,
    end_date: str | None = None,
    timezone: str | None = None,
) -> pd.DataFrame:
    """Fetch daily Open-Meteo weather for one city and return the normalized rows."""
    cfg = _extract_runtime_config()
    start_date = start_date or cfg["start_date"]
    end_date = end_date or cfg["end_date"]
    timezone = timezone or cfg["timezone"]
    request_timeout_seconds = cfg["request_timeout_seconds"]
    max_retries = cfg["max_retries"]
    backoff_base_seconds = cfg["backoff_base_seconds"]
    jitter_max_seconds = cfg["jitter_max_seconds"]

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": DAILY_VARS,
        # Local timezone to align with sale_date.
        "timezone": timezone,
    }

    logger.debug(
        "[EXTRACT_API] Request city=%s start=%s end=%s",
        city,
        start_date,
        end_date,
    )

    payload = _request_json_with_retry(
        city=city,
        url=url,
        params=params,
        timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base_seconds,
        jitter_max_seconds=jitter_max_seconds,
    )

    # Defensive payload validation
    daily = payload.get("daily")
    if not isinstance(daily, dict) or "time" not in daily:
        logger.error("[EXTRACT_API] Unexpected response structure for city=%s", city)
        raise ValueError(f"Unexpected API response structure for {city}")
    daily_payload = cast(dict[str, Any], daily)

    df = pd.DataFrame(
        {
            "date": daily_payload["time"],
            "city": city,
            "temp_c": daily_payload["temperature_2m_mean"],
            "precip_mm": daily_payload["precipitation_sum"],
            "precip_hours": daily_payload["precipitation_hours"],
            "weather_code": daily_payload["weather_code"],
        }
    )

    logger.debug("[EXTRACT_API] City=%s rows=%s", city, len(df))
    return df


def extract_weather_api() -> None:
    """Extract weather for all configured cities and save a deterministic RAW CSV."""
    cfg = _extract_runtime_config()
    cities = cfg["cities"]
    start_date = cfg["start_date"]
    end_date = cfg["end_date"]
    timezone = cfg["timezone"]
    max_workers = max(1, cfg["max_workers"])

    logger.info("[EXTRACT_API] Starting weather extraction for %s cities", len(cities))

    city_items = list(cities.items())

    if len(city_items) <= 1 or max_workers == 1:
        frames = [
            fetch_city_weather(
                city,
                lat,
                lon,
                start_date=start_date,
                end_date=end_date,
                timezone=timezone,
            )
            for city, (lat, lon) in city_items
        ]
    else:
        worker_count = min(max_workers, len(city_items))
        logger.info(
            "[EXTRACT_API] Parallel city fetch enabled workers=%s", worker_count
        )
        frames_by_city: dict[str, pd.DataFrame] = {}
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_city = {
                executor.submit(
                    fetch_city_weather,
                    city,
                    lat,
                    lon,
                    start_date=start_date,
                    end_date=end_date,
                    timezone=timezone,
                ): city
                for city, (lat, lon) in city_items
            }
            for future in as_completed(future_to_city):
                city = future_to_city[future]
                frames_by_city[city] = future.result()

        # Keep output deterministic (same order as config).
        frames = [frames_by_city[city] for city, _ in city_items]

    weather_df = pd.concat(frames, ignore_index=True)

    # Ensures the directory exists only when we are going to write.
    ensure_raw_dir(cfg["base"])

    weather_df.to_csv(cfg["out"], index=False)
    logger.info("[EXTRACT_API] Saved %s rows to %s", len(weather_df), cfg["out"])


def extract_csv(sources: dict[str, Path]) -> dict[str, pd.DataFrame]:
    """Read RAW CSV files into DataFrames.

    Raises `FileNotFoundError` when a declared source is missing, wraps
    `pd.errors.EmptyDataError` as `ValueError`, and lets `pd.errors.ParserError`
    propagate for malformed CSV content.
    """

    logger.info("[EXTRACT_CSV] Starting CSV extraction for %s sources", len(sources))

    # Existence validation (avoids silent errors)
    for name, path in sources.items():
        if not path.exists():
            logger.error("[EXTRACT_CSV] Missing source file for '%s': %s", name, path)
            raise FileNotFoundError(f"Missing source file for '{name}': {path}")

    results: dict[str, pd.DataFrame] = {}

    for name, path in sources.items():
        try:
            logger.debug("[EXTRACT_CSV] Reading %s from %s", name, path)
            df = pd.read_csv(path)

        except pd.errors.EmptyDataError:
            logger.exception("[EXTRACT_CSV] Empty CSV file for '%s': %s", name, path)
            raise ValueError(f"The CSV file is empty: {path}")

        results[name] = df
        logger.debug(
            "[EXTRACT_CSV] Loaded %s rows=%s cols=%s", name, df.shape[0], df.shape[1]
        )

    logger.info("[EXTRACT_CSV] CSV extraction finished")
    return results
