from __future__ import annotations

import pytest

import etl.settings as settings_module


@pytest.fixture(autouse=True)
def clear_settings_cache():
    settings_module.clear_settings_cache()
    yield
    settings_module.clear_settings_cache()


def test_get_settings_uses_defaults_when_no_config_candidates(monkeypatch, tmp_path):
    missing_1 = tmp_path / "missing_1.yaml"
    missing_2 = tmp_path / "missing_2.yaml"

    monkeypatch.setattr(
        settings_module,
        "_candidate_config_paths",
        lambda explicit_path=None: [missing_1, missing_2],
    )

    settings = settings_module.get_settings()

    assert settings == settings_module.DEFAULT_SETTINGS
    assert settings is not settings_module.DEFAULT_SETTINGS


def test_get_settings_merges_partial_yaml_with_defaults(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        """
paths:
  raw_dir: data/custom_raw
weather:
  cities:
    valencia:
      lat: 39.4699
      lon: -0.3763
""".strip() + "\n",
        encoding="utf-8",
    )

    settings = settings_module.get_settings(str(cfg))

    assert settings["paths"]["raw_dir"] == "data/custom_raw"
    assert (
        settings["paths"]["silver_dir"]
        == settings_module.DEFAULT_SETTINGS["paths"]["silver_dir"]
    )
    assert settings["logging"]["file"] == "logs/etl/pipeline.log"
    assert settings["logging"]["max_bytes"] == 2_000_000
    assert settings["logging"]["backup_count"] == 5
    assert settings["logging"]["level"] == "INFO"
    assert settings["exports"]["mart_formats"] == ["parquet"]
    assert settings["exports"]["star_schema_formats"] == ["parquet"]
    assert "Madrid" in settings["weather"]["cities"]
    assert settings["weather"]["cities"]["valencia"]["lat"] == pytest.approx(39.4699)


def test_get_settings_reads_from_env_var_path(tmp_path, monkeypatch):
    cfg = tmp_path / "env_config.yaml"
    cfg.write_text(
        """
paths:
  duckdb_path: data/custom.duckdb
""".strip() + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("RETAIL_ETL_CONFIG", str(cfg))

    settings = settings_module.get_settings()

    assert settings["paths"]["duckdb_path"] == "data/custom.duckdb"


def test_get_settings_resolves_relative_paths_with_base_dir_env(tmp_path, monkeypatch):
    cfg = tmp_path / "env_config.yaml"
    cfg.write_text(
        """
paths:
  raw_dir: data/custom_raw
  duckdb_path: data/custom.duckdb
""".strip() + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("RETAIL_ETL_CONFIG", str(cfg))
    monkeypatch.setenv("RETAIL_ETL_BASE_DIR", "/opt/airflow/project")

    settings = settings_module.get_settings()

    assert settings["paths"]["raw_dir"] == "/opt/airflow/project/data/custom_raw"
    assert settings["paths"]["duckdb_path"] == "/opt/airflow/project/data/custom.duckdb"


def test_get_settings_raises_for_non_mapping_yaml_root(tmp_path):
    cfg = tmp_path / "bad_config.yaml"
    cfg.write_text("- one\n- two\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must contain a mapping"):
        settings_module.get_settings(str(cfg))


def test_get_settings_raises_for_invalid_weather_date(tmp_path):
    cfg = tmp_path / "bad_config.yaml"
    cfg.write_text(
        """
weather:
  start_date: not-a-date
""".strip() + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid settings configuration"):
        settings_module.get_settings(str(cfg))


def test_get_settings_raises_for_invalid_weather_date_range(tmp_path):
    cfg = tmp_path / "bad_config.yaml"
    cfg.write_text(
        """
weather:
  start_date: 2025-04-01
  end_date: 2025-03-31
""".strip() + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid settings configuration"):
        settings_module.get_settings(str(cfg))


def test_get_settings_raises_for_invalid_max_workers(tmp_path):
    cfg = tmp_path / "bad_config.yaml"
    cfg.write_text(
        """
weather:
  request:
    max_workers: 0
""".strip() + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid settings configuration"):
        settings_module.get_settings(str(cfg))


def test_get_settings_raises_for_invalid_export_format(tmp_path):
    cfg = tmp_path / "bad_config.yaml"
    cfg.write_text(
        """
exports:
  mart_formats:
    - json
""".strip() + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid settings configuration"):
        settings_module.get_settings(str(cfg))


def test_get_settings_raises_for_unknown_config_key(tmp_path):
    cfg = tmp_path / "bad_config.yaml"
    cfg.write_text(
        """
weather:
  request:
    max_workerz: 4
""".strip() + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid settings configuration"):
        settings_module.get_settings(str(cfg))
