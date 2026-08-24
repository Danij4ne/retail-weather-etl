import pandas as pd
import pytest

from etl.settings import clear_settings_cache
from etl.validations import QualityGateError, enforce_quality_report, post_clean_checks


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    clear_settings_cache()
    yield
    clear_settings_cache()


def _write_quality_config(tmp_path, monkeypatch, **quality_values: float) -> None:
    lines = ["quality:"]
    lines.extend(f"  {key}: {value}" for key, value in quality_values.items())
    config_path = tmp_path / "quality_config.yaml"
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.setenv("RETAIL_ETL_CONFIG", str(config_path))
    clear_settings_cache()


def _rejected_sales_frame(count: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sale_id": list(range(900, 900 + count)),
            "customer_id": [1] * count,
            "product_id": [1001] * count,
            "sale_date": pd.to_datetime(["2024-03-01"] * count),
            "quantity": [1] * count,
            "discount": [0.0] * count,
            "reject_reasons": ["invalid_quantity"] * count,
        }
    )


def test_post_clean_checks_accepts_valid_inputs(valid_cleaned_and_rejected_for_checks):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    report = post_clean_checks(cleaned, rejected)
    assert report["status"] == "passed"
    assert report["critical_failures"] == []
    assert "metrics" in report
    assert set(report) == {"status", "critical_failures", "warnings", "metrics"}
    assert report["warnings"] == []
    assert report["metrics"]["weather"]["sales_with_unknown_city"] == 0
    assert report["metrics"]["weather"]["sales_with_no_weather_match"] == 0
    assert report["metrics"]["weather"]["weather_coverage_rate"] == pytest.approx(1.0)


def test_post_clean_checks_raises_on_duplicated_customer_id(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    duplicated = cleaned["customers"].iloc[[0]].copy()
    cleaned["customers"] = cleaned["customers"].copy()
    cleaned["customers"] = pd.concat(
        [cleaned["customers"], duplicated], ignore_index=True
    )

    with pytest.raises(QualityGateError, match="duplicated customer_id"):
        post_clean_checks(cleaned, rejected)


def test_post_clean_checks_warns_when_price_coverage_is_below_threshold(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    cleaned["products"] = cleaned["products"].copy()
    cleaned["products"].loc[cleaned["products"]["product_id"] == 1002, "price"] = pd.NA
    cleaned["products"].loc[
        cleaned["products"]["product_id"] == 1002, "is_price_valid"
    ] = False

    report = post_clean_checks(cleaned, rejected, fail_on_critical=False)

    price_warnings = [
        warning
        for warning in report["warnings"]
        if "without valid product price" in warning
    ]

    assert report["status"] == "passed"
    assert len(price_warnings) == 1
    assert "below configured threshold" in price_warnings[0]


def test_post_clean_checks_does_not_warn_when_price_coverage_meets_threshold(
    valid_cleaned_and_rejected_for_checks, tmp_path, monkeypatch
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks
    _write_quality_config(tmp_path, monkeypatch, min_valid_price_coverage=0.40)

    cleaned["products"] = cleaned["products"].copy()
    cleaned["products"].loc[cleaned["products"]["product_id"] == 1002, "price"] = pd.NA
    cleaned["products"].loc[
        cleaned["products"]["product_id"] == 1002, "is_price_valid"
    ] = False

    report = post_clean_checks(cleaned, rejected, fail_on_critical=False)

    assert report["metrics"]["weather"]["missing_valid_price_rows"] == 1
    assert report["metrics"]["weather"]["price_coverage_rate"] == pytest.approx(0.5)
    assert not any(
        "without valid product price" in warning for warning in report["warnings"]
    )


def test_post_clean_checks_does_not_warn_when_reject_rate_meets_threshold(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks
    rejected = {"sales": _rejected_sales_frame(1)}

    report = post_clean_checks(cleaned, rejected, fail_on_critical=False)

    assert report["metrics"]["sales"]["reject_rate"] == pytest.approx(1 / 3)
    assert not any("high reject rate" in warning for warning in report["warnings"])


def test_post_clean_checks_warns_when_reject_rate_exceeds_threshold(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks
    rejected = {"sales": _rejected_sales_frame(2)}

    report = post_clean_checks(cleaned, rejected, fail_on_critical=False)

    assert report["metrics"]["sales"]["reject_rate"] == pytest.approx(0.5)
    assert any("high reject rate" in warning for warning in report["warnings"])


def test_post_clean_checks_separates_unknown_city_from_weather_gap(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    cleaned["customers"] = cleaned["customers"].copy()
    cleaned["customers"].loc[cleaned["customers"]["customer_id"] == 2, "city"] = pd.NA

    report = post_clean_checks(cleaned, rejected, fail_on_critical=False)

    weather_metrics = report["metrics"]["weather"]

    assert weather_metrics["sales_with_unknown_city"] == 1
    assert weather_metrics["eligible_sales_for_weather_match"] == 1
    assert weather_metrics["sales_with_no_weather_match"] == 0
    assert weather_metrics["weather_coverage_rate"] == pytest.approx(1.0)
    assert any("unknown customer city" in warning for warning in report["warnings"])
    assert not any(
        "without weather match among sales with known city" in warning
        for warning in report["warnings"]
    )


def test_post_clean_checks_does_not_warn_when_unknown_city_rate_meets_threshold(
    valid_cleaned_and_rejected_for_checks, tmp_path, monkeypatch
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks
    _write_quality_config(tmp_path, monkeypatch, max_unknown_city_rate=0.60)

    cleaned["customers"] = cleaned["customers"].copy()
    cleaned["customers"].loc[cleaned["customers"]["customer_id"] == 2, "city"] = pd.NA

    report = post_clean_checks(cleaned, rejected, fail_on_critical=False)

    assert report["metrics"]["weather"]["sales_with_unknown_city"] == 1
    assert report["metrics"]["weather"]["unknown_city_rate"] == pytest.approx(0.5)
    assert not any("unknown customer city" in warning for warning in report["warnings"])


def test_post_clean_checks_counts_weather_gap_only_for_known_city_sales(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    cleaned["weather_daily"] = cleaned["weather_daily"].copy()
    cleaned["weather_daily"] = (
        cleaned["weather_daily"]
        .loc[cleaned["weather_daily"]["city"] != "Barcelona"]
        .reset_index(drop=True)
    )

    report = post_clean_checks(cleaned, rejected, fail_on_critical=False)

    weather_metrics = report["metrics"]["weather"]

    assert weather_metrics["sales_with_unknown_city"] == 0
    assert weather_metrics["eligible_sales_for_weather_match"] == 2
    assert weather_metrics["sales_with_no_weather_match"] == 1
    assert weather_metrics["weather_coverage_rate"] == pytest.approx(0.5)
    assert any(
        "without weather match among sales with known city" in warning
        for warning in report["warnings"]
    )


def test_post_clean_checks_raises_on_orphan_customer_id(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    cleaned["sales"] = cleaned["sales"].copy()
    cleaned["sales"].loc[0, "customer_id"] = 9999

    with pytest.raises(QualityGateError, match="orphan customer_id"):
        post_clean_checks(cleaned, rejected)


def test_post_clean_checks_raises_on_orphan_product_id(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    cleaned["sales"] = cleaned["sales"].copy()
    cleaned["sales"].loc[0, "product_id"] = 9999

    with pytest.raises(QualityGateError, match="orphan product_id"):
        post_clean_checks(cleaned, rejected)


def test_post_clean_checks_raises_when_products_contract_is_missing_column(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    cleaned["products"] = cleaned["products"].drop(columns=["is_price_valid"])

    with pytest.raises(QualityGateError, match="products.silver contract violation"):
        post_clean_checks(cleaned, rejected)


def test_post_clean_checks_raises_when_sales_contract_has_wrong_date_dtype(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    cleaned["sales"] = cleaned["sales"].copy()
    cleaned["sales"]["sale_date"] = cleaned["sales"]["sale_date"].dt.strftime(
        "%Y-%m-%d"
    )

    with pytest.raises(QualityGateError, match="sales.silver contract violation"):
        post_clean_checks(cleaned, rejected)


def test_post_clean_checks_raises_when_products_contract_has_invalid_price_state(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    cleaned["products"] = cleaned["products"].copy()
    cleaned["products"].loc[0, "is_price_valid"] = False

    with pytest.raises(
        QualityGateError,
        match=(
            "products.silver contract violation: price must be positive when "
            "is_price_valid is true and NA otherwise"
        ),
    ):
        post_clean_checks(cleaned, rejected)


def test_post_clean_checks_raises_when_weather_contract_has_duplicate_key(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    duplicated = cleaned["weather_daily"].iloc[[0]].copy()
    cleaned["weather_daily"] = pd.concat(
        [cleaned["weather_daily"], duplicated], ignore_index=True
    )

    with pytest.raises(
        QualityGateError, match="weather_daily.silver contract violation"
    ):
        post_clean_checks(cleaned, rejected)


def test_post_clean_checks_raises_when_weather_city_is_null(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    cleaned["weather_daily"] = cleaned["weather_daily"].copy()
    cleaned["weather_daily"].loc[0, "city"] = pd.NA

    with pytest.raises(
        QualityGateError,
        match="weather_daily.silver contract violation: city must not contain nulls",
    ):
        post_clean_checks(cleaned, rejected)


def test_post_clean_checks_raises_when_weather_date_is_null(
    valid_cleaned_and_rejected_for_checks,
):
    cleaned, rejected = valid_cleaned_and_rejected_for_checks

    cleaned["weather_daily"] = cleaned["weather_daily"].copy()
    cleaned["weather_daily"].loc[0, "date"] = pd.NaT

    with pytest.raises(
        QualityGateError,
        match="weather_daily.silver contract violation: date must not contain nulls",
    ):
        post_clean_checks(cleaned, rejected)


def test_enforce_quality_report_returns_passing_report():
    report = {
        "status": "passed",
        "critical_failures": [],
        "warnings": [],
        "metrics": {},
    }

    assert enforce_quality_report(report) is report


def test_enforce_quality_report_raises_when_status_is_not_passed():
    report = {
        "status": "failed",
        "critical_failures": ["duplicated sale_id"],
        "warnings": [],
        "metrics": {},
    }

    with pytest.raises(QualityGateError, match="duplicated sale_id") as exc_info:
        enforce_quality_report(report)

    assert exc_info.value.report is report
