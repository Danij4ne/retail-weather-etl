from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_AIRFLOW_AVAILABLE = importlib.util.find_spec("airflow") is not None

if _AIRFLOW_AVAILABLE:
    from airflow.models import DagBag


def _load_airflow_dag_module():
    dag_path = Path(__file__).resolve().parents[2] / "dags" / "etl_dag.py"
    spec = importlib.util.spec_from_file_location(
        "retail_weather_etl_dag_module", dag_path
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.skipif(not _AIRFLOW_AVAILABLE, reason="airflow is not installed")
def test_airflow_dag_parses_and_has_expected_tasks():
    dags_folder = Path(__file__).resolve().parents[2] / "dags"
    dag_bag = DagBag(dag_folder=str(dags_folder), include_examples=False)

    assert dag_bag.import_errors == {}
    assert "retail_weather_etl" in dag_bag.dags

    dag = dag_bag.get_dag("retail_weather_etl")
    assert dag is not None
    assert {"extract", "transform", "load", "cleanup"}.issubset(dag.task_dict.keys())
    assert "cleanup" in dag.task_dict["load"].downstream_task_ids


@pytest.mark.skipif(not _AIRFLOW_AVAILABLE, reason="airflow is not installed")
def test_airflow_fetch_weather_env_flag_defaults_to_false(monkeypatch):
    dag_module = _load_airflow_dag_module()
    monkeypatch.delenv(dag_module.AIRFLOW_FETCH_WEATHER_ENV, raising=False)

    assert dag_module._env_flag(dag_module.AIRFLOW_FETCH_WEATHER_ENV) is False


@pytest.mark.skipif(not _AIRFLOW_AVAILABLE, reason="airflow is not installed")
def test_airflow_fetch_weather_env_flag_accepts_truthy_values(monkeypatch):
    dag_module = _load_airflow_dag_module()
    monkeypatch.setenv(dag_module.AIRFLOW_FETCH_WEATHER_ENV, "true")

    assert dag_module._env_flag(dag_module.AIRFLOW_FETCH_WEATHER_ENV) is True


@pytest.mark.skipif(not _AIRFLOW_AVAILABLE, reason="airflow is not installed")
def test_cleanup_rejects_stage_dir_outside_staging_root(tmp_path):
    dag_module = _load_airflow_dag_module()
    staging_root = tmp_path / "staging" / "airflow"
    staging_root.mkdir(parents=True)
    outsider = tmp_path / "elsewhere"
    outsider.mkdir()

    with pytest.raises(ValueError, match="Refusing to cleanup outside staging root"):
        dag_module._resolve_stage_dir_for_cleanup(
            str(outsider),
            staging_root=staging_root,
        )


@pytest.mark.skipif(not _AIRFLOW_AVAILABLE, reason="airflow is not installed")
def test_cleanup_rejects_staging_root_itself(tmp_path):
    dag_module = _load_airflow_dag_module()
    staging_root = tmp_path / "staging" / "airflow"
    staging_root.mkdir(parents=True)

    with pytest.raises(ValueError, match="Refusing to cleanup outside staging root"):
        dag_module._resolve_stage_dir_for_cleanup(
            str(staging_root),
            staging_root=staging_root,
        )


@pytest.mark.skipif(not _AIRFLOW_AVAILABLE, reason="airflow is not installed")
def test_cleanup_accepts_missing_child_under_staging_root(tmp_path):
    dag_module = _load_airflow_dag_module()
    staging_root = tmp_path / "staging" / "airflow"
    staging_root.mkdir(parents=True)
    child = staging_root / "run_abc"

    resolved = dag_module._resolve_stage_dir_for_cleanup(
        str(child),
        staging_root=staging_root,
    )

    assert resolved == child.resolve()
    assert not resolved.exists()
