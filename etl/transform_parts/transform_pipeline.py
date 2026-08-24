from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd

from etl.transform_parts.clean_customers import clean_customers
from etl.transform_parts.clean_products import clean_products
from etl.transform_parts.clean_sales import clean_sales
from etl.transform_parts.clean_weather import clean_weather_daily
from etl.transform_parts.normalization_config import get_normalization_config
from etl.transform_parts.save_outputs import save_silver_and_rejected
from etl.validations import post_clean_checks

logger = logging.getLogger("etl.transform")

DataFrameDict = dict[str, pd.DataFrame]
HooksDict = dict[str, Callable[..., Any]]


def _default_hooks() -> HooksDict:
    return {
        "clean_customers": clean_customers,
        "clean_products": clean_products,
        "clean_sales": clean_sales,
        "clean_weather": clean_weather_daily,
        "post_clean_checks": post_clean_checks,
        "save_outputs": save_silver_and_rejected,
    }


def transform(
    results: DataFrameDict,
    run_checks: bool = True,
    save_outputs: bool = True,
    *,
    hooks: HooksDict | None = None,
) -> tuple[DataFrameDict, DataFrameDict]:
    """
    Orchestrates all dataset transformations.
    Receives raw dataframes and returns cleaned ones.
    """
    hooks = hooks or _default_hooks()

    clean_customers_fn = hooks["clean_customers"]
    clean_products_fn = hooks["clean_products"]
    clean_sales_fn = hooks["clean_sales"]
    clean_weather_fn = hooks["clean_weather"]
    post_clean_checks_fn = hooks["post_clean_checks"]
    save_outputs_fn = hooks["save_outputs"]

    cleaned: DataFrameDict = {}
    rejected: DataFrameDict = {}
    city_map, category_map, leet_enabled, leet_map = get_normalization_config()

    customers_clean = clean_customers_fn(results["customers"], city_map=city_map)
    cleaned["customers"] = customers_clean

    products_clean = clean_products_fn(
        results["products"],
        category_map=category_map,
        leet_enabled=leet_enabled,
        leet_map=leet_map,
    )
    cleaned["products"] = products_clean

    sales_clean, sales_rejected = clean_sales_fn(
        results["sales"], customers_clean, products_clean
    )
    cleaned["sales"] = sales_clean
    rejected["sales"] = sales_rejected

    weather_clean = clean_weather_fn(results["weather_daily"])
    cleaned["weather_daily"] = weather_clean

    if run_checks:
        post_clean_checks_fn(cleaned, rejected)

    if save_outputs:
        save_outputs_fn(cleaned, rejected)

    logger.info(
        "[TRANSFORM] completed | cleaned_rows={customers:%s, products:%s, sales:%s, weather_daily:%s} rejected_rows={sales:%s}",
        len(cleaned["customers"]),
        len(cleaned["products"]),
        len(cleaned["sales"]),
        len(cleaned["weather_daily"]),
        len(rejected["sales"]),
    )

    return cleaned, rejected
