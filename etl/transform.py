from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from etl.transform_parts.clean_customers import clean_customers as _clean_customers
from etl.transform_parts.clean_products import clean_products as _clean_products
from etl.transform_parts.clean_sales import clean_sales
from etl.transform_parts.clean_weather import clean_weather_daily
from etl.transform_parts.normalization_config import get_normalization_config
from etl.transform_parts.profiling import profiling
from etl.transform_parts.save_outputs import save_silver_and_rejected
from etl.transform_parts.transform_pipeline import transform


def clean_customers(
    df: pd.DataFrame,
    city_map: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Clean customer rows using the configured city normalization map by default."""
    if city_map is None:
        city_map, _, _, _ = get_normalization_config()
    return _clean_customers(df, city_map=city_map)


def clean_products(
    df: pd.DataFrame,
    category_map: Mapping[str, str] | None = None,
    *,
    leet_enabled: bool | None = None,
    leet_map: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Clean product rows using configured category and leet normalization defaults."""
    _, cfg_category_map, cfg_leet_enabled, cfg_leet_map = get_normalization_config()

    if category_map is None:
        category_map = cfg_category_map
    if leet_enabled is None:
        leet_enabled = cfg_leet_enabled
    if leet_map is None:
        leet_map = cfg_leet_map

    return _clean_products(
        df,
        category_map=category_map,
        leet_enabled=leet_enabled,
        leet_map=leet_map,
    )


__all__ = [
    "clean_customers",
    "clean_products",
    "clean_sales",
    "clean_weather_daily",
    "profiling",
    "save_silver_and_rejected",
    "transform",
]
