"""Product cleaning rules for normalization, price coercion, and deduplication."""

from __future__ import annotations

import logging
from collections.abc import Mapping

import pandas as pd

logger = logging.getLogger("etl.transform")

UNKNOWN_CATEGORY = "unknown"


def _coerce_leet_map(leet_map: Mapping[str, str] | None) -> dict[str, str]:
    """Keep only valid one-character leet substitutions from configuration."""

    if leet_map is None:
        return {}

    normalized: dict[str, str] = {}
    for key, value in leet_map.items():
        key_s = str(key).strip()
        value_s = str(value).strip()
        if len(key_s) == 1 and len(value_s) == 1:
            normalized[key_s] = value_s
    return normalized


def _apply_leet_conservative(value: str, leet_map: Mapping[str, str]) -> str:
    """Apply leet normalization only when the token still looks like a word.

    Examples:
    - ``"b0tella"`` -> ``"botella"``
    - ``"usb3"`` -> ``"usb3"``
    - ``"3.0 liters"`` -> ``"3.0 liters"``
    """
    if not value:
        return value

    tokens = value.split(" ")
    normalized_tokens: list[str] = []

    for token in tokens:
        token_lower = token.lower()
        if not token_lower:
            normalized_tokens.append(token_lower)
            continue

        # Keep tokens that start/end with digits untouched to avoid false
        # positives in measurements or versions (e.g. 3.0, 10L, USB3).
        if token_lower[0].isdigit() or token_lower[-1].isdigit():
            normalized_tokens.append(token_lower)
            continue

        chars = list(token_lower)
        for i, char in enumerate(chars):
            if char not in leet_map:
                continue

            prev_char = chars[i - 1] if i > 0 else ""
            next_char = chars[i + 1] if i < len(chars) - 1 else ""
            if prev_char.isalpha() or next_char.isalpha():
                chars[i] = leet_map[char]

        normalized_tokens.append("".join(chars))

    return " ".join(normalized_tokens)


def clean_products(
    df: pd.DataFrame,
    category_map: Mapping[str, str] | None = None,
    *,
    leet_enabled: bool = True,
    leet_map: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Normalize, score, and deduplicate products while preserving invalid-price rows."""
    df = df.copy()

    df["product_id_raw"] = df["product_id"].astype("string").str.strip()
    df["product_id_num"] = pd.to_numeric(df["product_id_raw"], errors="coerce")
    invalid_product_id = (
        df["product_id_raw"].isna()
        | (df["product_id_raw"] == "")
        | df["product_id_num"].isna()
        | (df["product_id_num"] % 1 != 0)
    )
    dropped_invalid_pk = int(invalid_product_id.sum())
    if dropped_invalid_pk:
        logger.warning(
            "[TRANSFORM][products] dropping rows with invalid product_id=%s",
            dropped_invalid_pk,
        )
    df = df.loc[~invalid_product_id].copy()
    df["product_id_num"] = df["product_id_num"].astype("int64")

    dup_mask = df["product_id_num"].duplicated(keep=False)
    dup_products = df[dup_mask].sort_values("product_id_num")
    logger.debug(
        "[TRANSFORM][products] duplicated_product_id=%s",
        int(df["product_id_num"].duplicated().sum()),
    )
    if dup_mask.any():
        logger.debug(
            "[TRANSFORM][products] duplicated sample:\n%s", dup_products.head(10)
        )

    df["category_clean"] = (
        df["category"]
        .astype("string")
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )
    df.loc[df["category_clean"] == "", "category_clean"] = pd.NA

    normalized_leet_map = _coerce_leet_map(leet_map)

    product_name_clean = (
        df["product_name"]
        .astype("string")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    if leet_enabled and normalized_leet_map:
        product_name_clean = product_name_clean.apply(
            lambda value: (
                _apply_leet_conservative(str(value), normalized_leet_map)
                if pd.notna(value)
                else value
            )
        )
    else:
        product_name_clean = product_name_clean.str.lower()

    df["product_name_clean"] = product_name_clean
    df.loc[df["product_name_clean"] == "", "product_name_clean"] = pd.NA

    price_str = (
        df["price"]
        .astype("string")
        .str.strip()
        .str.replace(r"[^0-9,.\-]", "", regex=True)
        .str.replace(",", ".", regex=False)
    )
    df["price_num"] = pd.to_numeric(price_str, errors="coerce")

    df["_has_name"] = df["product_name_clean"].notna() & (
        df["product_name_clean"] != ""
    )
    df["_has_cat"] = df["category_clean"].notna()
    df["_has_price"] = df["price_num"].notna() & (df["price_num"] > 0)

    df["_score"] = (
        df["_has_name"].astype(int)
        + df["_has_cat"].astype(int)
        + df["_has_price"].astype(int)
    )

    dup_preview = df.loc[
        dup_mask,
        ["product_id", "product_id_num", "product_name", "category", "price", "_score"],
    ]
    if dup_mask.any():
        logger.debug(
            "[TRANSFORM][products] duplicates with score sample:\n%s",
            dup_preview.sort_values(
                ["product_id_num", "_score"], ascending=[True, False]
            ).head(10),
        )

    df_sorted = df.sort_values(
        by=["product_id_num", "_score", "price_num"],
        ascending=[True, False, False],
        kind="mergesort",
    )
    df_clean = df_sorted.drop_duplicates(subset=["product_id_num"], keep="first").copy()

    normalized_category_map = dict(category_map or {})
    canonical_categories = set(normalized_category_map.values()) | {UNKNOWN_CATEGORY}
    mapped_category = df_clean["category_clean"].replace(normalized_category_map)
    mapped_category = (
        mapped_category.astype("string")
        .str.strip()
        .str.lower()
        .fillna(UNKNOWN_CATEGORY)
    )
    mapped_category = mapped_category.where(
        mapped_category.isin(canonical_categories), UNKNOWN_CATEGORY
    )
    df_clean["category"] = mapped_category.str.title()
    df_clean["product_name"] = df_clean["product_name_clean"].str.title()

    df_clean["is_price_valid"] = df_clean["price_num"].notna() & (
        df_clean["price_num"] > 0
    )
    price = df_clean["price_num"].astype("Float64")
    df_clean["price"] = price.where(df_clean["is_price_valid"])
    df_clean["product_id"] = df_clean["product_id_num"].astype("int64")

    df_clean = df_clean.drop(
        columns=[
            "product_id_raw",
            "product_id_num",
            "category_clean",
            "product_name_clean",
            "price_num",
            "_has_name",
            "_has_cat",
            "_has_price",
            "_score",
        ]
    ).reset_index(drop=True)

    return df_clean
