from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from etl.settings import get_settings

logger = logging.getLogger("etl.transform")

SILVER_OUTPUT_SCHEMAS: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id", "first_name", "last_name", "city", "signup_date"),
    "products": ("product_id", "product_name", "category", "price", "is_price_valid"),
    "sales": (
        "sale_id",
        "customer_id",
        "product_id",
        "sale_date",
        "quantity",
        "discount",
    ),
    "weather_daily": (
        "date",
        "city",
        "temp_c",
        "precip_mm",
        "precip_hours",
        "weather_code",
    ),
}

REJECTED_OUTPUT_SCHEMAS: dict[str, tuple[str, ...]] = {
    "sales": (
        "sale_id",
        "customer_id",
        "product_id",
        "sale_date",
        "quantity",
        "discount",
        "reject_reasons",
    ),
}


def _project_output_schema(
    df: pd.DataFrame,
    dataset_name: str,
    expected_columns: tuple[str, ...],
) -> pd.DataFrame:
    extra_columns = [col for col in df.columns if col not in expected_columns]
    if extra_columns:
        logger.warning(
            "[TRANSFORM] Dropping extra output columns dataset=%s columns=%s",
            dataset_name,
            extra_columns,
        )

    return df.loc[:, list(expected_columns)]


def save_silver_and_rejected(
    cleaned: dict[str, pd.DataFrame],
    rejected: dict[str, pd.DataFrame],
) -> None:
    settings = get_settings()
    paths = settings.get("paths", {})

    out_silver = Path(paths.get("silver_dir", "data/processed/silver"))
    out_rejected = Path(paths.get("rejected_dir", "data/processed/rejected"))

    out_silver.mkdir(parents=True, exist_ok=True)
    out_rejected.mkdir(parents=True, exist_ok=True)

    for dataset_name, expected_columns in SILVER_OUTPUT_SCHEMAS.items():
        projected = _project_output_schema(
            cleaned[dataset_name],
            dataset_name,
            expected_columns,
        )
        projected.to_parquet(out_silver / f"{dataset_name}_clean.parquet", index=False)

    for dataset_name, expected_columns in REJECTED_OUTPUT_SCHEMAS.items():
        projected = _project_output_schema(
            rejected[dataset_name],
            f"rejected_{dataset_name}",
            expected_columns,
        )
        projected.to_csv(out_rejected / f"{dataset_name}_rejected.csv", index=False)

    logger.info(
        "[TRANSFORM] Saved outputs | silver_rows={customers:%s, products:%s, sales:%s, weather_daily:%s} rejected_rows={sales:%s}",
        len(cleaned["customers"]),
        len(cleaned["products"]),
        len(cleaned["sales"]),
        len(cleaned["weather_daily"]),
        len(rejected["sales"]),
    )
