"""Weather cleaning rules for type coercion, normalization, and duplicate resolution."""

from __future__ import annotations

import logging

import pandas as pd

from etl.transform_parts.date_parsing import parse_dates

logger = logging.getLogger("etl.transform")


def clean_weather_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize daily weather rows and keep the most complete row per (date, city)."""
    rows_in = len(df)
    df = df.copy()

    df["date"] = parse_dates(df["date"], dataset="weather")
    null_dates = int(df["date"].isna().sum())
    if null_dates:
        logger.debug("[TRANSFORM][weather] null_date_after_parse=%s", null_dates)

    df["city"] = (
        df["city"]
        .astype("string")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.lower()
        .str.title()
    )

    num_cols = ["temp_c", "precip_mm", "precip_hours"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "weather_code" in df.columns:
        df["weather_code"] = pd.to_numeric(df["weather_code"], errors="coerce").astype(
            "Int64"
        )

    if "precip_mm" in df.columns:
        neg_precip_mask = df["precip_mm"].notna() & (df["precip_mm"] < 0)
        neg_precip_count = int(neg_precip_mask.sum())
        if neg_precip_count:
            logger.warning(
                "[TRANSFORM][weather] coercing negative precip_mm to NA rows=%s",
                neg_precip_count,
            )
        df.loc[neg_precip_mask, "precip_mm"] = pd.NA

    rows_before_dedup = len(df)
    completeness_cols = [
        col
        for col in ("temp_c", "precip_mm", "precip_hours", "weather_code")
        if col in df.columns
    ]
    if completeness_cols:
        df["_completeness_score"] = df[completeness_cols].notna().sum(axis=1)
        # Prefer the most complete weather row per (date, city); when duplicates
        # tie on completeness, stable sorting preserves the first row from input.
        df = df.sort_values(
            ["city", "date", "_completeness_score"],
            ascending=[True, True, False],
            kind="mergesort",
        )
    else:
        df = df.sort_values(["city", "date"], kind="mergesort")

    df = df.drop_duplicates(subset=["date", "city"], keep="first").reset_index(
        drop=True
    )
    if "_completeness_score" in df.columns:
        df = df.drop(columns=["_completeness_score"])
    dedup_removed = rows_before_dedup - len(df)
    if dedup_removed:
        logger.warning(
            "[TRANSFORM][weather] dropped duplicated (date, city) count=%s",
            dedup_removed,
        )

    logger.debug(
        "[TRANSFORM][weather] completed rows_in=%s rows_out=%s",
        rows_in,
        len(df),
    )

    return df
