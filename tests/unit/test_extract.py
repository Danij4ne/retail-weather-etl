from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import requests

import etl.extract as extract_module


def _runtime_cfg_with_request(**overrides: object) -> dict[str, object]:
    cfg: dict[str, object] = {
        "base": Path("data/raw"),
        "out": Path("data/raw/weather_daily.csv"),
        "cities": {"Madrid": (40.4168, -3.7038)},
        "start_date": "2025-01-01",
        "end_date": "2025-01-03",
        "timezone": "Europe/Madrid",
        "request_timeout_seconds": 30,
        "max_retries": 3,
        "backoff_base_seconds": 1.0,
        "jitter_max_seconds": 0.5,
        "max_workers": 4,
    }
    cfg.update(overrides)
    return cfg


def test_build_cities_skips_invalid_entries_with_warnings(
    caplog: pytest.LogCaptureFixture,
):
    weather_cfg = {
        "cities": {
            "madrid": {"lat": 40.4168, "lon": -3.7038},
            "broken_payload": "not-a-dict",
            "missing_lon": {"lat": 41.0},
            "bad_coords": {"lat": "north", "lon": 2.17},
        }
    }

    with caplog.at_level("WARNING", logger=extract_module.logger.name):
        cities = extract_module._build_cities(weather_cfg)

    assert cities == {"Madrid": (40.4168, -3.7038)}
    assert "reason=payload_not_mapping" in caplog.text
    assert "reason=missing_lat_lon" in caplog.text
    assert "reason=invalid_lat_lon" in caplog.text


def test_fetch_city_weather_retries_timeout_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        extract_module,
        "_extract_runtime_config",
        lambda: _runtime_cfg_with_request(max_retries=2, jitter_max_seconds=0.0),
    )

    payload: dict[str, object] = {
        "daily": {
            "time": ["2025-01-01", "2025-01-02"],
            "temperature_2m_mean": [12.1, 11.0],
            "precipitation_sum": [0.2, 0.0],
            "precipitation_hours": [1.0, 0.0],
            "weather_code": [3, 1],
        }
    }

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return payload

    calls = {"n": 0}

    def _fake_get(*args: object, **kwargs: object) -> _Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise requests.exceptions.Timeout("timeout")
        return _Response()

    sleep_calls: list[float] = []

    monkeypatch.setattr("etl.extract.requests.get", _fake_get)
    monkeypatch.setattr(
        "etl.extract.time.sleep", lambda s: sleep_calls.append(float(s))
    )
    monkeypatch.setattr("etl.extract.random.uniform", lambda a, b: 0.0)

    df = extract_module.fetch_city_weather("Madrid", 40.4168, -3.7038)

    assert calls["n"] == 2
    assert sleep_calls == [1.0]
    assert list(df.columns) == [
        "date",
        "city",
        "temp_c",
        "precip_mm",
        "precip_hours",
        "weather_code",
    ]
    assert len(df) == 2


def test_fetch_city_weather_does_not_retry_non_retryable_http(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        extract_module,
        "_extract_runtime_config",
        lambda: _runtime_cfg_with_request(max_retries=3, jitter_max_seconds=0.0),
    )

    response = requests.Response()
    response.status_code = 400

    calls = {"n": 0}

    def _fake_get(*args: object, **kwargs: object):
        calls["n"] += 1
        raise requests.exceptions.HTTPError("bad request", response=response)

    sleep_calls: list[float] = []

    monkeypatch.setattr("etl.extract.requests.get", _fake_get)
    monkeypatch.setattr(
        "etl.extract.time.sleep", lambda s: sleep_calls.append(float(s))
    )
    monkeypatch.setattr("etl.extract.random.uniform", lambda a, b: 0.0)

    with pytest.raises(requests.exceptions.HTTPError):
        extract_module.fetch_city_weather("Madrid", 40.4168, -3.7038)

    assert calls["n"] == 1
    assert sleep_calls == []


def test_extract_weather_api_parallel_output_keeps_city_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    out_csv = tmp_path / "weather_daily.csv"
    cfg = _runtime_cfg_with_request(
        base=tmp_path,
        out=out_csv,
        cities={
            "Madrid": (40.4168, -3.7038),
            "Barcelona": (41.3851, 2.1734),
            "Valencia": (39.4699, -0.3763),
        },
        max_workers=4,
    )
    monkeypatch.setattr(extract_module, "_extract_runtime_config", lambda: cfg)

    def _fake_fetch(
        city: str,
        lat: float,
        lon: float,
        start_date: str | None = None,
        end_date: str | None = None,
        timezone: str | None = None,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": ["2025-01-01"],
                "city": [city],
                "temp_c": [10.0],
                "precip_mm": [0.0],
                "precip_hours": [0.0],
                "weather_code": [1],
            }
        )

    monkeypatch.setattr(extract_module, "fetch_city_weather", _fake_fetch)

    extract_module.extract_weather_api()

    got = pd.read_csv(out_csv)
    assert got["city"].tolist() == ["Madrid", "Barcelona", "Valencia"]


def test_extract_weather_api_parallel_city_failure_bubbles_and_skips_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    out_csv = tmp_path / "weather_daily.csv"
    cfg = _runtime_cfg_with_request(
        base=tmp_path,
        out=out_csv,
        cities={
            "Madrid": (40.4168, -3.7038),
            "Barcelona": (41.3851, 2.1734),
        },
        max_workers=4,
    )
    monkeypatch.setattr(extract_module, "_extract_runtime_config", lambda: cfg)

    def _fake_fetch(
        city: str,
        lat: float,
        lon: float,
        start_date: str | None = None,
        end_date: str | None = None,
        timezone: str | None = None,
    ) -> pd.DataFrame:
        if city == "Barcelona":
            raise RuntimeError("barcelona failed")
        return pd.DataFrame(
            {
                "date": ["2025-01-01"],
                "city": [city],
                "temp_c": [10.0],
                "precip_mm": [0.0],
                "precip_hours": [0.0],
                "weather_code": [1],
            }
        )

    monkeypatch.setattr(extract_module, "fetch_city_weather", _fake_fetch)

    with pytest.raises(RuntimeError, match="barcelona failed"):
        extract_module.extract_weather_api()

    assert not out_csv.exists()


def test_extract_csv_raises_when_source_file_is_missing(tmp_path: Path):
    missing = tmp_path / "missing.csv"
    sources = {"customers": missing}

    with pytest.raises(FileNotFoundError, match="Missing source file"):
        extract_module.extract_csv(sources)


def test_extract_csv_raises_when_csv_is_empty(tmp_path: Path):
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("", encoding="utf-8")
    sources = {"customers": empty_csv}

    with pytest.raises(ValueError, match="CSV file is empty|The CSV file is empty"):
        extract_module.extract_csv(sources)


def test_extract_csv_raises_for_malformed_csv(tmp_path: Path):
    malformed_csv = tmp_path / "malformed.csv"
    malformed_csv.write_text('a,b\n1,"2\n3,4\n', encoding="utf-8")
    sources = {"customers": malformed_csv}

    with pytest.raises(pd.errors.ParserError):
        extract_module.extract_csv(sources)
