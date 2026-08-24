"""Customer cleaning rules for normalization, scoring, and deterministic deduplication."""

from __future__ import annotations

import logging
from collections.abc import Mapping

import pandas as pd

from etl.transform_parts.date_parsing import parse_dates

logger = logging.getLogger("etl.transform")


def clean_customers(
    df: pd.DataFrame,
    city_map: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Normalize, score, and deduplicate customers by the best row per customer id."""
    df = df.copy()

    df["customer_id_raw"] = df["customer_id"].astype("string").str.strip()
    df["customer_id_num"] = pd.to_numeric(df["customer_id_raw"], errors="coerce")
    invalid_customer_id = (
        df["customer_id_raw"].isna()
        | (df["customer_id_raw"] == "")
        | df["customer_id_num"].isna()
        | (df["customer_id_num"] % 1 != 0)
    )
    dropped_invalid_pk = int(invalid_customer_id.sum())
    if dropped_invalid_pk:
        logger.warning(
            "[TRANSFORM][customers] dropping rows with invalid customer_id=%s",
            dropped_invalid_pk,
        )
    df = df.loc[~invalid_customer_id].copy()
    df["customer_id_num"] = df["customer_id_num"].astype("int64")

    dup_mask = df["customer_id_num"].duplicated(keep=False)
    dup_customers = df[dup_mask].sort_values("customer_id_num")
    logger.debug(
        "[TRANSFORM][customers] duplicated_customer_id=%s",
        int(df["customer_id_num"].duplicated().sum()),
    )
    if dup_mask.any():
        logger.debug(
            "[TRANSFORM][customers] duplicated sample:\n%s", dup_customers.head(10)
        )

    df["city_clean"] = (
        df["city"]
        .astype("string")
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )
    df.loc[df["city_clean"] == "", "city_clean"] = pd.NA

    df["first_name_clean"] = (
        df["first_name"]
        .astype("string")
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )
    df.loc[df["first_name_clean"] == "", "first_name_clean"] = pd.NA

    df["last_name_clean"] = (
        df["last_name"]
        .astype("string")
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )
    df.loc[df["last_name_clean"] == "", "last_name_clean"] = pd.NA

    df["signup_date_parsed"] = parse_dates(df["signup_date"], dataset="customers")

    df["_has_city"] = df["city_clean"].notna()
    df["_has_date"] = df["signup_date_parsed"].notna()
    df["_has_first"] = df["first_name_clean"].notna()
    df["_has_last"] = df["last_name_clean"].notna()

    df["_score"] = (
        df["_has_city"].astype(int)
        + df["_has_date"].astype(int)
        + df["_has_first"].astype(int)
        + df["_has_last"].astype(int)
    )

    dup_preview = df.loc[
        dup_mask,
        [
            "customer_id",
            "customer_id_num",
            "first_name",
            "last_name",
            "city",
            "signup_date",
            "_score",
        ],
    ]
    if dup_mask.any():
        logger.debug(
            "[TRANSFORM][customers] duplicates with score sample:\n%s",
            dup_preview.sort_values(
                ["customer_id_num", "_score"], ascending=[True, False]
            ).head(10),
        )

    df_sorted = df.sort_values(
        by=["customer_id_num", "_score", "signup_date_parsed"],
        ascending=[True, False, False],
        kind="mergesort",
    )
    df_clean = df_sorted.drop_duplicates(
        subset=["customer_id_num"], keep="first"
    ).copy()

    normalized_city_map = dict(city_map or {})
    df_clean["city"] = df_clean["city_clean"].replace(normalized_city_map).str.title()

    df_clean["first_name"] = df_clean["first_name_clean"].str.title()
    df_clean["last_name"] = df_clean["last_name_clean"].str.title()

    df_clean["signup_date"] = df_clean["signup_date_parsed"]
    df_clean["customer_id"] = df_clean["customer_id_num"].astype("int64")

    df_clean = df_clean.drop(
        columns=[
            "customer_id_raw",
            "customer_id_num",
            "city_clean",
            "first_name_clean",
            "last_name_clean",
            "signup_date_parsed",
            "_has_city",
            "_has_date",
            "_has_first",
            "_has_last",
            "_score",
        ]
    ).reset_index(drop=True)

    return df_clean
