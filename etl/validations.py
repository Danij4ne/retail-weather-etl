"""Post-transform quality gate and serializable validation report contracts."""

from __future__ import annotations

import logging
from typing import Any, Literal, TypedDict

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaError, SchemaErrors

from etl.schemas.silver import (
    CUSTOMERS_SILVER_SCHEMA,
    PRODUCTS_SILVER_SCHEMA,
    SALES_SILVER_SCHEMA,
    WEATHER_DAILY_SILVER_SCHEMA,
)
from etl.settings import get_settings

logger = logging.getLogger("etl.validations")


class ValidationRowsMetrics(TypedDict):
    customers: int
    products: int
    sales_clean: int
    sales_rejected: int
    weather: int


class ValidationCustomersMetrics(TypedDict):
    null_customer_id: int
    dup_customer_id: int


class ValidationProductsMetrics(TypedDict):
    null_product_id: int
    dup_product_id: int
    invalid_price_le_0: int
    digits_in_name: int


class ValidationSalesMetrics(TypedDict):
    null_sale_id: int
    dup_sale_id: int
    invalid_qty: int
    invalid_disc: int
    unknown_cust_fk: int
    unknown_prod_fk: int
    reject_rate: float
    max_reject_rate: float


class ValidationWeatherMetrics(TypedDict):
    null_weather_date: int
    dup_weather_key: int
    neg_precip: int
    sales_with_unknown_city: int
    unknown_city_rate: float
    eligible_sales_for_weather_match: int
    sales_with_no_weather_match: int
    weather_coverage_rate: float
    missing_valid_price_rows: int
    price_coverage_rate: float
    min_valid_price_coverage: float
    max_unknown_city_rate: float


class QualityThresholds(TypedDict):
    max_reject_rate: float
    min_valid_price_coverage: float
    max_unknown_city_rate: float


class ValidationMetrics(TypedDict):
    rows: ValidationRowsMetrics
    customers: ValidationCustomersMetrics
    products: ValidationProductsMetrics
    sales: ValidationSalesMetrics
    weather: ValidationWeatherMetrics


class QualityReport(TypedDict):
    status: Literal["passed", "failed"]
    critical_failures: list[str]
    warnings: list[str]
    metrics: ValidationMetrics


class QualityGateError(Exception):
    """Raised when post-clean validation finds blocking quality issues."""

    report: QualityReport

    def __init__(self, message: str, report: QualityReport):
        super().__init__(message)
        self.report = report


def _missing_required_columns(
    df: pd.DataFrame,
    *,
    dataset_name: str,
    required_columns: tuple[str, ...],
) -> list[str]:
    """Return contract failures for missing required columns in one dataset."""

    missing = [column for column in required_columns if column not in df.columns]
    if not missing:
        return []
    return [f"{dataset_name}.silver contract violation: missing columns {missing}"]


def _validate_silver_schema(
    df: pd.DataFrame,
    *,
    dataset_name: str,
    schema: pa.DataFrameSchema,
) -> list[str]:
    """Validate one cleaned dataset against a Pandera silver schema."""

    failures = _missing_required_columns(
        df,
        dataset_name=dataset_name,
        required_columns=tuple(schema.columns.keys()),
    )
    if failures:
        return failures

    try:
        schema.validate(df, lazy=True)
    except SchemaErrors as exc:
        schema_failures: list[str] = []
        seen_messages: set[str] = set()
        for check in exc.failure_cases["check"].dropna():
            message = f"{dataset_name}.silver contract violation: {check}"
            if message not in seen_messages:
                seen_messages.add(message)
                schema_failures.append(message)
        return schema_failures
    except SchemaError as exc:
        if isinstance(exc.check, str) and exc.check:
            return [f"{dataset_name}.silver contract violation: {exc.check}"]
        return [f"{dataset_name}.silver contract violation: {exc}"]

    return []


def _check_customers_contract(df: pd.DataFrame) -> list[str]:
    """Validate the structural silver contract for cleaned customers."""

    return _validate_silver_schema(
        df,
        dataset_name="customers",
        schema=CUSTOMERS_SILVER_SCHEMA,
    )


def _check_products_contract(df: pd.DataFrame) -> list[str]:
    """Validate the structural silver contract for cleaned products."""

    return _validate_silver_schema(
        df,
        dataset_name="products",
        schema=PRODUCTS_SILVER_SCHEMA,
    )


def _check_sales_contract(df: pd.DataFrame) -> list[str]:
    """Validate the structural silver contract for cleaned sales."""

    return _validate_silver_schema(
        df,
        dataset_name="sales",
        schema=SALES_SILVER_SCHEMA,
    )


def _check_weather_contract(df: pd.DataFrame) -> list[str]:
    """Validate the structural silver contract for cleaned weather rows."""

    return _validate_silver_schema(
        df,
        dataset_name="weather_daily",
        schema=WEATHER_DAILY_SILVER_SCHEMA,
    )


def _check_silver_contracts(cleaned: dict[str, Any]) -> list[str]:
    """Validate the structural silver contract for every cleaned dataset."""

    return [
        *_check_customers_contract(cleaned["customers"]),
        *_check_products_contract(cleaned["products"]),
        *_check_sales_contract(cleaned["sales"]),
        *_check_weather_contract(cleaned["weather_daily"]),
    ]


def _clamp_unit_interval(value: float) -> float:
    """Clamp a ratio-like setting into the inclusive ``[0, 1]`` interval."""

    return min(1.0, max(0.0, value))


def _get_quality_thresholds() -> QualityThresholds:
    """Resolve warning floors calibrated against the synthetic source baseline."""

    settings = get_settings()
    quality_cfg = settings.get("quality", {}) if isinstance(settings, dict) else {}
    if not isinstance(quality_cfg, dict):
        quality_cfg = {}

    return {
        "max_reject_rate": _clamp_unit_interval(
            float(quality_cfg.get("max_reject_rate", 0.45))
        ),
        "min_valid_price_coverage": _clamp_unit_interval(
            float(quality_cfg.get("min_valid_price_coverage", 0.90))
        ),
        "max_unknown_city_rate": _clamp_unit_interval(
            float(quality_cfg.get("max_unknown_city_rate", 0.08))
        ),
    }


def _build_rows_metrics(
    customers_clean: pd.DataFrame,
    products_clean: pd.DataFrame,
    sales_clean: pd.DataFrame,
    sales_rejected: pd.DataFrame,
    weather_clean: pd.DataFrame,
) -> ValidationRowsMetrics:
    """Build the top-level row-count metrics block for the quality report."""

    return {
        "customers": int(len(customers_clean)),
        "products": int(len(products_clean)),
        "sales_clean": int(len(sales_clean)),
        "sales_rejected": int(len(sales_rejected)),
        "weather": int(len(weather_clean)),
    }


def _compute_customers_metrics(
    customers_clean: pd.DataFrame,
    *,
    detail: bool,
) -> ValidationCustomersMetrics:
    """Compute customer integrity metrics and optional debug distributions."""

    null_customer_id = int(customers_clean["customer_id"].isna().sum())
    dup_customer_id = int(customers_clean["customer_id"].duplicated().sum())
    logger.debug(
        "[CHECK][customers] rows=%s null_customer_id=%s duplicated_customer_id=%s",
        len(customers_clean),
        null_customer_id,
        dup_customer_id,
    )
    if detail:
        logger.debug(
            "[CHECK][customers] city_distribution=\n%s",
            customers_clean["city"].value_counts(dropna=False),
        )
        logger.debug(
            "[CHECK][customers] signup_date_dtype=%s",
            customers_clean["signup_date"].dtype,
        )
    return {
        "null_customer_id": null_customer_id,
        "dup_customer_id": dup_customer_id,
    }


def _compute_products_metrics(
    products_clean: pd.DataFrame,
    *,
    detail: bool,
) -> ValidationProductsMetrics:
    """Compute product integrity metrics and remaining digit-based name anomalies."""

    null_product_id = int(products_clean["product_id"].isna().sum())
    dup_product_id = int(products_clean["product_id"].duplicated().sum())
    invalid_price = int(
        (products_clean["price"].notna() & (products_clean["price"] <= 0)).sum()
    )
    logger.debug(
        "[CHECK][products] rows=%s null_product_id=%s duplicated_product_id=%s invalid_price_le_0=%s",
        len(products_clean),
        null_product_id,
        dup_product_id,
        invalid_price,
    )
    if detail:
        logger.debug(
            "[CHECK][products] category_distribution=\n%s",
            products_clean["category"].value_counts(dropna=False),
        )
        logger.debug(
            "[CHECK][products] product_name_sample=\n%s",
            products_clean["product_name"].head(20),
        )

    digits_in_name = int(
        products_clean["product_name"]
        .astype("string")
        .str.contains(r"\d", regex=True)
        .sum()
    )

    return {
        "null_product_id": null_product_id,
        "dup_product_id": dup_product_id,
        "invalid_price_le_0": invalid_price,
        "digits_in_name": digits_in_name,
    }


def _compute_sales_metrics(
    sales_clean: pd.DataFrame,
    sales_rejected: pd.DataFrame,
    customers_clean: pd.DataFrame,
    products_clean: pd.DataFrame,
    *,
    max_reject_rate: float,
) -> ValidationSalesMetrics:
    """Compute sales integrity metrics, orphan FKs, and reject rate."""

    null_sale_id = int(sales_clean["sale_id"].isna().sum())
    dup_sale_id = int(sales_clean["sale_id"].duplicated().sum())
    invalid_qty = int(
        (sales_clean["quantity"].isna() | (sales_clean["quantity"] <= 0)).sum()
    )
    invalid_disc = int(
        (
            sales_clean["discount"].isna()
            | (sales_clean["discount"] < 0)
            | (sales_clean["discount"] > 100)
        ).sum()
    )
    unknown_cust_fk = int(
        (~sales_clean["customer_id"].isin(customers_clean["customer_id"])).sum()
    )
    unknown_prod_fk = int(
        (~sales_clean["product_id"].isin(products_clean["product_id"])).sum()
    )
    logger.debug(
        "[CHECK][sales] rows_clean=%s rows_rejected=%s null_sale_id=%s duplicated_sale_id=%s invalid_qty=%s invalid_discount=%s unknown_customer_fk=%s unknown_product_fk=%s",
        len(sales_clean),
        len(sales_rejected),
        null_sale_id,
        dup_sale_id,
        invalid_qty,
        invalid_disc,
        unknown_cust_fk,
        unknown_prod_fk,
    )

    total_sales = len(sales_clean) + len(sales_rejected)
    reject_rate = (len(sales_rejected) / total_sales) if total_sales > 0 else 0.0

    return {
        "null_sale_id": null_sale_id,
        "dup_sale_id": dup_sale_id,
        "invalid_qty": invalid_qty,
        "invalid_disc": invalid_disc,
        "unknown_cust_fk": unknown_cust_fk,
        "unknown_prod_fk": unknown_prod_fk,
        "reject_rate": reject_rate,
        "max_reject_rate": max_reject_rate,
    }


def _compute_weather_coverage_metrics(
    sales_clean: pd.DataFrame,
    customers_clean: pd.DataFrame,
    weather_clean: pd.DataFrame,
) -> tuple[int, int, int, float]:
    """Measure weather coverage only over clean sales with known customer city."""

    try:
        sales_with_city = sales_clean.merge(
            customers_clean[["customer_id", "city"]], on="customer_id", how="left"
        )
        merged = sales_with_city.merge(
            weather_clean[["date", "city"]].drop_duplicates(),
            left_on=["sale_date", "city"],
            right_on=["date", "city"],
            how="left",
            indicator=True,
        )

        sales_with_unknown_city = int(sales_with_city["city"].isna().sum())
        sales_with_known_city = merged[merged["city"].notna()].copy()
        sales_with_no_weather_match = int(
            (sales_with_known_city["_merge"] == "left_only").sum()
        )
        eligible_sales_for_weather_match = int(len(sales_with_known_city))
        weather_coverage_rate = (
            (
                (eligible_sales_for_weather_match - sales_with_no_weather_match)
                / eligible_sales_for_weather_match
            )
            if eligible_sales_for_weather_match > 0
            else 0.0
        )
        return (
            sales_with_unknown_city,
            eligible_sales_for_weather_match,
            sales_with_no_weather_match,
            weather_coverage_rate,
        )
    except (KeyError, TypeError, ValueError) as exc:
        logger.error("[CHECK][weather] coverage_metrics_unavailable error=%s", exc)
        return (0, 0, 0, 0.0)


def _compute_price_coverage_metrics(
    sales_clean: pd.DataFrame,
    products_clean: pd.DataFrame,
) -> tuple[int, float]:
    """Measure how many clean sales can be priced from valid product rows."""

    try:
        sales_with_price = sales_clean.merge(
            products_clean[["product_id", "price"]],
            on="product_id",
            how="left",
        )
        valid_price_rows = int(
            (sales_with_price["price"].notna() & (sales_with_price["price"] > 0)).sum()
        )
        missing_valid_price_rows = len(sales_clean) - valid_price_rows
        price_coverage_rate = (
            (valid_price_rows / len(sales_clean)) if len(sales_clean) > 0 else 0.0
        )
        return (int(missing_valid_price_rows), float(price_coverage_rate))
    except (KeyError, TypeError, ValueError) as exc:
        logger.error("[CHECK][products] price_coverage_unavailable error=%s", exc)
        return (int(len(sales_clean)), 0.0)


def _compute_weather_metrics(
    sales_clean: pd.DataFrame,
    customers_clean: pd.DataFrame,
    products_clean: pd.DataFrame,
    weather_clean: pd.DataFrame,
    *,
    min_valid_price_coverage: float,
    max_unknown_city_rate: float,
) -> ValidationWeatherMetrics:
    """Compute weather integrity plus downstream coverage metrics for clean sales."""

    null_weather_date = int(weather_clean["date"].isna().sum())
    dup_weather_key = int(weather_clean[["date", "city"]].duplicated().sum())
    neg_precip = int(
        (weather_clean["precip_mm"].notna() & (weather_clean["precip_mm"] < 0)).sum()
    )
    logger.debug(
        "[CHECK][weather] rows=%s null_date=%s duplicated_date_city=%s negative_precip_mm=%s",
        len(weather_clean),
        null_weather_date,
        dup_weather_key,
        neg_precip,
    )

    (
        sales_with_unknown_city,
        eligible_sales_for_weather_match,
        sales_with_no_weather_match,
        weather_coverage_rate,
    ) = _compute_weather_coverage_metrics(sales_clean, customers_clean, weather_clean)
    missing_valid_price_rows, price_coverage_rate = _compute_price_coverage_metrics(
        sales_clean, products_clean
    )
    sales_clean_n = len(sales_clean)
    unknown_city_rate = (
        sales_with_unknown_city / sales_clean_n if sales_clean_n else 0.0
    )

    return {
        "null_weather_date": null_weather_date,
        "dup_weather_key": dup_weather_key,
        "neg_precip": neg_precip,
        "sales_with_unknown_city": sales_with_unknown_city,
        "unknown_city_rate": unknown_city_rate,
        "eligible_sales_for_weather_match": eligible_sales_for_weather_match,
        "sales_with_no_weather_match": sales_with_no_weather_match,
        "weather_coverage_rate": weather_coverage_rate,
        "missing_valid_price_rows": missing_valid_price_rows,
        "price_coverage_rate": price_coverage_rate,
        "min_valid_price_coverage": min_valid_price_coverage,
        "max_unknown_city_rate": max_unknown_city_rate,
    }


def _build_critical_failures(
    *,
    customers_metrics: ValidationCustomersMetrics,
    products_metrics: ValidationProductsMetrics,
    sales_metrics: ValidationSalesMetrics,
    contract_failures: list[str],
) -> list[str]:
    """Build the blocking quality-gate failures that must stop the load."""

    critical_failures: list[str] = []

    if sales_metrics["null_sale_id"] > 0:
        critical_failures.append(
            f"null sale_id after cleaning ({sales_metrics['null_sale_id']})"
        )
    if sales_metrics["dup_sale_id"] > 0:
        critical_failures.append(
            f"duplicated sale_id after cleaning ({sales_metrics['dup_sale_id']})"
        )
    if customers_metrics["null_customer_id"] > 0:
        critical_failures.append(
            f"null customer_id after cleaning ({customers_metrics['null_customer_id']})"
        )
    if customers_metrics["dup_customer_id"] > 0:
        critical_failures.append(
            f"duplicated customer_id ({customers_metrics['dup_customer_id']})"
        )
    if products_metrics["null_product_id"] > 0:
        critical_failures.append(
            f"null product_id after cleaning ({products_metrics['null_product_id']})"
        )
    if products_metrics["dup_product_id"] > 0:
        critical_failures.append(
            f"duplicated product_id ({products_metrics['dup_product_id']})"
        )
    if sales_metrics["unknown_cust_fk"] > 0:
        critical_failures.append(
            f"orphan customer_id in sales ({sales_metrics['unknown_cust_fk']})"
        )
    if sales_metrics["unknown_prod_fk"] > 0:
        critical_failures.append(
            f"orphan product_id in sales ({sales_metrics['unknown_prod_fk']})"
        )

    critical_failures.extend(contract_failures)
    return critical_failures


def _build_warning_messages(
    *,
    products_metrics: ValidationProductsMetrics,
    sales_metrics: ValidationSalesMetrics,
    weather_metrics: ValidationWeatherMetrics,
) -> list[str]:
    """Build non-blocking warnings that should remain visible in logs and lineage."""

    warning_messages: list[str] = []

    if sales_metrics["reject_rate"] > sales_metrics["max_reject_rate"]:
        warning_messages.append(
            "high reject rate "
            f"({sales_metrics['reject_rate'] * 100:.2f}%) above configured threshold "
            f"({sales_metrics['reject_rate'] * 100:.2f}% > {sales_metrics['max_reject_rate'] * 100:.2f}%)"
        )

    if weather_metrics["unknown_city_rate"] > weather_metrics["max_unknown_city_rate"]:
        warning_messages.append(
            f"{weather_metrics['sales_with_unknown_city']} clean sales rows with unknown customer city "
            f"(rate={weather_metrics['unknown_city_rate'] * 100:.2f}%) above configured threshold "
            f"({weather_metrics['unknown_city_rate'] * 100:.2f}% > {weather_metrics['max_unknown_city_rate'] * 100:.2f}%)"
        )

    if weather_metrics["sales_with_no_weather_match"] > 0:
        warning_messages.append(
            f"{weather_metrics['sales_with_no_weather_match']} clean sales rows without weather match among sales with known city "
            f"(coverage={weather_metrics['weather_coverage_rate'] * 100:.2f}%)"
        )

    if (
        weather_metrics["price_coverage_rate"]
        < weather_metrics["min_valid_price_coverage"]
    ):
        warning_messages.append(
            f"{weather_metrics['missing_valid_price_rows']} clean sales rows without valid product price "
            f"(coverage={weather_metrics['price_coverage_rate'] * 100:.2f}%) "
            "below configured threshold "
            f"({weather_metrics['price_coverage_rate'] * 100:.2f}% < {weather_metrics['min_valid_price_coverage'] * 100:.2f}%)"
        )

    if products_metrics["digits_in_name"] > 0:
        warning_messages.append(
            f"{products_metrics['digits_in_name']} product_name values still contain digits"
        )

    return warning_messages


def _build_quality_report(
    *,
    rows_metrics: ValidationRowsMetrics,
    customers_metrics: ValidationCustomersMetrics,
    products_metrics: ValidationProductsMetrics,
    sales_metrics: ValidationSalesMetrics,
    weather_metrics: ValidationWeatherMetrics,
    critical_failures: list[str],
    warning_messages: list[str],
) -> QualityReport:
    """Assemble the final serializable quality report returned by the gate."""

    return {
        "status": "failed" if critical_failures else "passed",
        "critical_failures": critical_failures,
        "warnings": warning_messages,
        "metrics": {
            "rows": rows_metrics,
            "customers": customers_metrics,
            "products": products_metrics,
            "sales": sales_metrics,
            "weather": weather_metrics,
        },
    }


def post_clean_checks(
    cleaned: dict[str, Any],
    rejected: dict[str, Any],
    detail: bool = False,
    *,
    fail_on_critical: bool = True,
) -> QualityReport:
    """Validate cleaned silver datasets, build a serializable report, and optionally fail."""

    customers_clean = cleaned["customers"]
    products_clean = cleaned["products"]
    sales_clean = cleaned["sales"]
    sales_rejected = rejected["sales"]
    weather_clean = cleaned["weather_daily"]
    quality_thresholds = _get_quality_thresholds()

    logger.info("[CHECK] Stage started")

    rows_metrics = _build_rows_metrics(
        customers_clean,
        products_clean,
        sales_clean,
        sales_rejected,
        weather_clean,
    )
    customers_metrics = _compute_customers_metrics(customers_clean, detail=detail)
    products_metrics = _compute_products_metrics(products_clean, detail=detail)
    sales_metrics = _compute_sales_metrics(
        sales_clean,
        sales_rejected,
        customers_clean,
        products_clean,
        max_reject_rate=quality_thresholds["max_reject_rate"],
    )
    weather_metrics = _compute_weather_metrics(
        sales_clean,
        customers_clean,
        products_clean,
        weather_clean,
        min_valid_price_coverage=quality_thresholds["min_valid_price_coverage"],
        max_unknown_city_rate=quality_thresholds["max_unknown_city_rate"],
    )
    contract_failures = _check_silver_contracts(cleaned)

    # ------------------ QUALITY GATE ------------------
    logger.debug("[CHECK] quality_gate_start")
    critical_failures = _build_critical_failures(
        customers_metrics=customers_metrics,
        products_metrics=products_metrics,
        sales_metrics=sales_metrics,
        contract_failures=contract_failures,
    )
    warning_messages = _build_warning_messages(
        products_metrics=products_metrics,
        sales_metrics=sales_metrics,
        weather_metrics=weather_metrics,
    )

    for warning_message in warning_messages:
        logger.warning(warning_message)

    report = _build_quality_report(
        rows_metrics=rows_metrics,
        customers_metrics=customers_metrics,
        products_metrics=products_metrics,
        sales_metrics=sales_metrics,
        weather_metrics=weather_metrics,
        critical_failures=critical_failures,
        warning_messages=warning_messages,
    )

    if critical_failures and fail_on_critical:
        msg = "CRITICAL quality gate failed: " + "; ".join(critical_failures)
        logger.error(msg)
        raise QualityGateError(msg, report)

    logger.info(
        "[CHECK] Stage completed status=%s | rows={customers:%s, products:%s, sales_clean:%s, sales_rejected:%s, weather:%s}",
        report["status"],
        rows_metrics["customers"],
        rows_metrics["products"],
        rows_metrics["sales_clean"],
        rows_metrics["sales_rejected"],
        rows_metrics["weather"],
    )

    return report


def enforce_quality_report(report: QualityReport) -> QualityReport:
    """Raise when a serialized quality report is not a passing gate result."""

    if report.get("status") == "passed":
        return report

    failures = report.get("critical_failures") or []
    detail = "; ".join(str(item) for item in failures) if failures else str(
        report.get("status")
    )
    raise QualityGateError(f"CRITICAL quality gate failed: {detail}", report)
