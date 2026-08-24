import etl.transform as transform_module
import etl.transform_parts.normalization_config as normalization_config_module
import etl.transform_parts.transform_pipeline as transform_pipeline_module


def test_transform_returns_expected_outputs(raw_results):
    cleaned, rejected = transform_module.transform(
        raw_results, run_checks=False, save_outputs=False
    )

    assert set(cleaned.keys()) == {"customers", "products", "sales", "weather_daily"}
    assert set(rejected.keys()) == {"sales"}
    assert len(cleaned["sales"]) == 1
    assert "reject_reasons" in rejected["sales"].columns


def test_transform_runs_optional_steps_when_enabled(raw_results, monkeypatch):
    calls = {"checks": 0, "save": 0}

    def fake_checks(cleaned, rejected):
        calls["checks"] += 1
        assert "sales" in cleaned
        assert "sales" in rejected

    def fake_save(cleaned, rejected):
        calls["save"] += 1
        assert "customers" in cleaned
        assert "sales" in rejected

    monkeypatch.setattr(transform_pipeline_module, "post_clean_checks", fake_checks)
    monkeypatch.setattr(
        transform_pipeline_module, "save_silver_and_rejected", fake_save
    )

    transform_module.transform(raw_results, run_checks=True, save_outputs=True)

    assert calls == {"checks": 1, "save": 1}


def test_transform_uses_normalization_maps_from_settings(raw_results, monkeypatch):
    monkeypatch.setattr(
        normalization_config_module,
        "get_settings",
        lambda: {
            "normalization": {
                "city_map": {"madríd": "sevilla"},
                "category_map": {"accesories": "camping gear"},
            }
        },
    )

    cleaned, _ = transform_module.transform(
        raw_results, run_checks=False, save_outputs=False
    )

    customer_1 = (
        cleaned["customers"].loc[cleaned["customers"]["customer_id"] == 1].iloc[0]
    )
    product_1001 = (
        cleaned["products"].loc[cleaned["products"]["product_id"] == 1001].iloc[0]
    )

    assert customer_1["city"] == "Sevilla"
    assert product_1001["category"] == "Camping Gear"


def test_transform_falls_back_to_default_normalization_maps(raw_results, monkeypatch):
    monkeypatch.setattr(normalization_config_module, "get_settings", lambda: {})

    cleaned, _ = transform_module.transform(
        raw_results, run_checks=False, save_outputs=False
    )

    customer_1 = (
        cleaned["customers"].loc[cleaned["customers"]["customer_id"] == 1].iloc[0]
    )
    product_1001 = (
        cleaned["products"].loc[cleaned["products"]["product_id"] == 1001].iloc[0]
    )

    assert customer_1["city"] == "Madrid"
    assert product_1001["category"] == "Accessories"


def test_transform_can_disable_leet_from_settings(raw_results, monkeypatch):
    monkeypatch.setattr(
        normalization_config_module,
        "get_settings",
        lambda: {
            "normalization": {
                "city_map": {"madríd": "madrid"},
                "category_map": {"accesories": "accessories"},
                "product_name": {
                    "leet_enabled": False,
                    "leet_map": {"0": "o"},
                },
            }
        },
    )

    cleaned, _ = transform_module.transform(
        raw_results, run_checks=False, save_outputs=False
    )
    product_1001 = (
        cleaned["products"].loc[cleaned["products"]["product_id"] == 1001].iloc[0]
    )

    assert product_1001["product_name"] == "B0Tella"
