"""Pandera schemas for canonical silver datasets."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_integer_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_string_dtype,
)


def _is_textual_series(series: pd.Series[Any]) -> bool:
    """Return whether a series is acceptable for text-like silver columns."""

    return bool(is_string_dtype(series.dtype) or is_object_dtype(series.dtype))


def _is_integer_like(series: pd.Series[Any]) -> bool:
    """Return whether a series uses an integer-like dtype."""

    return bool(is_integer_dtype(series))


def _is_numeric_like(series: pd.Series[Any]) -> bool:
    """Return whether a series uses a numeric dtype."""

    return bool(is_numeric_dtype(series))


def _is_boolean_like(series: pd.Series[Any]) -> bool:
    """Return whether a series uses a boolean-like dtype."""

    return bool(is_bool_dtype(series))


def _is_datetime_like(series: pd.Series[Any]) -> bool:
    """Return whether a series uses a datetime-like dtype."""

    return bool(is_datetime64_any_dtype(series))


def _has_no_nulls(series: pd.Series[Any]) -> bool:
    """Return whether a series contains no null values."""

    return bool(series.notna().all())


def _no_nulls_check(error: str) -> pa.Check:
    """Build a null-rejection check that actually sees nulls.

    Pandera defaults to ignore_na=True and drops NAs before calling the
    check function, which would make ``_has_no_nulls`` a no-op.
    """

    return pa.Check(_has_no_nulls, error=error, ignore_na=False)


def _products_price_state_is_valid(df: pd.DataFrame) -> bool:
    """Enforce the cleaned product price contract used downstream."""

    valid_price_mismatch = df["is_price_valid"].eq(True) & (
        df["price"].isna() | (df["price"] <= 0)
    )
    invalid_price_mismatch = df["is_price_valid"].eq(False) & df["price"].notna()
    return not bool(valid_price_mismatch.any() or invalid_price_mismatch.any())


def _weather_key_is_unique(df: pd.DataFrame) -> bool:
    """Enforce uniqueness for the natural weather grain (date, city)."""

    return not bool(df[["date", "city"]].duplicated().any())


CUSTOMERS_SILVER_SCHEMA = pa.DataFrameSchema(
    {
        "customer_id": pa.Column(
            checks=pa.Check(_is_integer_like, error="customer_id must be integer-like"),
            nullable=True,
        ),
        "first_name": pa.Column(
            checks=pa.Check(_is_textual_series, error="first_name must be text-like"),
            nullable=True,
        ),
        "last_name": pa.Column(
            checks=pa.Check(_is_textual_series, error="last_name must be text-like"),
            nullable=True,
        ),
        "city": pa.Column(
            checks=pa.Check(_is_textual_series, error="city must be text-like"),
            nullable=True,
        ),
        "signup_date": pa.Column(
            checks=pa.Check(
                _is_datetime_like,
                error="signup_date must be datetime-like",
            ),
            nullable=True,
        ),
    },
    strict=False,
)


PRODUCTS_SILVER_SCHEMA = pa.DataFrameSchema(
    {
        "product_id": pa.Column(
            checks=pa.Check(_is_integer_like, error="product_id must be integer-like"),
            nullable=True,
        ),
        "product_name": pa.Column(
            checks=pa.Check(
                _is_textual_series,
                error="product_name must be text-like",
            ),
            nullable=True,
        ),
        "category": pa.Column(
            checks=pa.Check(_is_textual_series, error="category must be text-like"),
            nullable=True,
        ),
        "price": pa.Column(
            checks=pa.Check(_is_numeric_like, error="price must be numeric"),
            nullable=True,
        ),
        "is_price_valid": pa.Column(
            checks=pa.Check(
                _is_boolean_like,
                error="is_price_valid must be boolean-like",
            ),
            nullable=True,
        ),
    },
    checks=[
        pa.Check(
            _products_price_state_is_valid,
            error=(
                "price must be positive when is_price_valid is true and NA otherwise"
            ),
        )
    ],
    strict=False,
)


SALES_SILVER_SCHEMA = pa.DataFrameSchema(
    {
        "sale_id": pa.Column(
            checks=pa.Check(_is_integer_like, error="sale_id must be integer-like"),
            nullable=True,
        ),
        "customer_id": pa.Column(
            checks=pa.Check(_is_integer_like, error="customer_id must be integer-like"),
            nullable=True,
        ),
        "product_id": pa.Column(
            checks=pa.Check(_is_integer_like, error="product_id must be integer-like"),
            nullable=True,
        ),
        "sale_date": pa.Column(
            checks=pa.Check(_is_datetime_like, error="sale_date must be datetime-like"),
            nullable=True,
        ),
        "quantity": pa.Column(
            checks=pa.Check(_is_integer_like, error="quantity must be integer-like"),
            nullable=True,
        ),
        "discount": pa.Column(
            checks=pa.Check(_is_numeric_like, error="discount must be numeric"),
            nullable=True,
        ),
    },
    strict=False,
)


WEATHER_DAILY_SILVER_SCHEMA = pa.DataFrameSchema(
    {
        "date": pa.Column(
            checks=[
                pa.Check(_is_datetime_like, error="date must be datetime-like"),
                _no_nulls_check("date must not contain nulls"),
            ],
            nullable=True,
        ),
        "city": pa.Column(
            checks=[
                pa.Check(_is_textual_series, error="city must be text-like"),
                _no_nulls_check("city must not contain nulls"),
            ],
            nullable=True,
        ),
        "temp_c": pa.Column(
            checks=pa.Check(_is_numeric_like, error="temp_c must be numeric"),
            nullable=True,
        ),
        "precip_mm": pa.Column(
            checks=pa.Check(_is_numeric_like, error="precip_mm must be numeric"),
            nullable=True,
        ),
        "precip_hours": pa.Column(
            checks=pa.Check(_is_numeric_like, error="precip_hours must be numeric"),
            nullable=True,
        ),
        "weather_code": pa.Column(
            checks=pa.Check(
                _is_integer_like,
                error="weather_code must be integer-like",
            ),
            nullable=True,
        ),
    },
    checks=[pa.Check(_weather_key_is_unique, error="(date, city) must be unique")],
    strict=False,
)
