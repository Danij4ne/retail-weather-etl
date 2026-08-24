"""Sales cleaning rules for parsing, scoring, deduplication, and reject splitting."""

from __future__ import annotations

import logging
from typing import Any, cast

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from etl.transform_parts.date_parsing import parse_dates

logger = logging.getLogger("etl.transform")

# Official reporting contract: left-to-right order in `reject_reasons`.
REJECT_REASONS_PRIORITY = (
    "missing_sale_id",
    "invalid_sale_id",
    "missing_customer_id",
    "invalid_customer_id",
    "missing_product_id",
    "invalid_product_id",
    "invalid_sale_date",
    "unknown_customer_id",
    "unknown_product_id",
    "invalid_quantity",
    "invalid_discount",
    "discount_out_of_range",
)

AUXILIARY_SALES_COLS = (
    "sale_id_raw",
    "customer_id_raw",
    "product_id_raw",
    "discount_raw",
    "sale_id_num",
    "customer_id_num",
    "product_id_num",
    "sale_date_parsed",
    "quantity_num",
    "discount_num",
    "_sale_id_missing",
    "_cust_id_missing",
    "_prod_id_missing",
    "_sale_id_invalid",
    "_cust_id_invalid",
    "_prod_id_invalid",
    "_disc_missing",
    "_disc_invalid",
    "_has_date",
    "_qty_is_int",
    "_has_qty",
    "_has_disc",
    "_has_cust",
    "_has_prod",
    "_score",
)

CLEAN_ONLY_DROP_COLS = ("reject_reasons",)


def _build_reject_reasons(reason_flags: pd.DataFrame) -> pd.Series:
    """Build pipe-separated reject reasons while preserving column order."""
    if reason_flags.empty:
        return pd.Series(index=reason_flags.index, dtype="string")

    reject_reasons: NDArray[Any] = np.full(len(reason_flags), "", dtype=object)
    for reason in reason_flags.columns:
        mask = reason_flags[reason].to_numpy(dtype=bool, copy=False)
        reject_reasons = np.where(
            mask,
            np.where(
                reject_reasons == "",
                reason,
                reject_reasons + "|" + reason,
            ),
            reject_reasons,
        )

    reject_reasons_series = pd.Series(
        reject_reasons,
        index=reason_flags.index,
        dtype="string",
    )
    return reject_reasons_series.mask(reject_reasons_series == "", pd.NA)


def _parse_sales_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Parse raw identifier, date, quantity, and discount fields into typed auxiliaries."""

    df["sale_id_raw"] = df["sale_id"].astype("string").str.strip()
    df["customer_id_raw"] = df["customer_id"].astype("string").str.strip()
    df["product_id_raw"] = df["product_id"].astype("string").str.strip()
    df["discount_raw"] = df["discount"].astype("string").str.strip()

    df["sale_id_num"] = pd.to_numeric(df["sale_id_raw"], errors="coerce")
    df["customer_id_num"] = pd.to_numeric(df["customer_id_raw"], errors="coerce")
    df["product_id_num"] = pd.to_numeric(df["product_id_raw"], errors="coerce")
    df["sale_date_parsed"] = parse_dates(df["sale_date"], dataset="sales")
    df["quantity_num"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["discount_num"] = pd.to_numeric(df["discount_raw"], errors="coerce")

    df["_sale_id_missing"] = df["sale_id_raw"].isna() | (df["sale_id_raw"] == "")
    df["_cust_id_missing"] = df["customer_id_raw"].isna() | (
        df["customer_id_raw"] == ""
    )
    df["_prod_id_missing"] = df["product_id_raw"].isna() | (df["product_id_raw"] == "")

    df["_sale_id_invalid"] = (~df["_sale_id_missing"]) & (
        df["sale_id_num"].isna() | (df["sale_id_num"] % 1 != 0)
    )
    df["_cust_id_invalid"] = (~df["_cust_id_missing"]) & (
        df["customer_id_num"].isna() | (df["customer_id_num"] % 1 != 0)
    )
    df["_prod_id_invalid"] = (~df["_prod_id_missing"]) & (
        df["product_id_num"].isna() | (df["product_id_num"] % 1 != 0)
    )

    df["_disc_missing"] = df["discount_raw"].isna() | (df["discount_raw"] == "")
    df["_disc_invalid"] = (~df["_disc_missing"]) & df["discount_num"].isna()
    df.loc[df["_disc_missing"], "discount_num"] = 0
    return df


def _flag_sales_quality(
    df: pd.DataFrame,
    valid_customers: set[int],
    valid_products: set[int],
) -> pd.DataFrame:
    """Compute reusable quality flags and the deduplication score for each sale row."""

    df["_has_date"] = df["sale_date_parsed"].notna()
    df["_qty_is_int"] = df["quantity_num"].notna() & (df["quantity_num"] % 1 == 0)
    df["_has_qty"] = df["_qty_is_int"] & (df["quantity_num"] > 0)
    df["_has_disc"] = (
        (~df["_disc_invalid"]) & (df["discount_num"] >= 0) & (df["discount_num"] <= 100)
    )
    df["_has_cust"] = (
        (~df["_cust_id_missing"])
        & (~df["_cust_id_invalid"])
        & df["customer_id_num"].isin(valid_customers)
    )
    df["_has_prod"] = (
        (~df["_prod_id_missing"])
        & (~df["_prod_id_invalid"])
        & df["product_id_num"].isin(valid_products)
    )

    df["_score"] = (
        df["_has_date"].astype(int)
        + df["_has_qty"].astype(int)
        + df["_has_disc"].astype(int)
        + df["_has_cust"].astype(int)
        + df["_has_prod"].astype(int)
    )
    return df


def _log_duplicate_sales(df: pd.DataFrame) -> None:
    """Log duplicate sale_id candidates and their scores for troubleshooting."""

    valid_sale_id_for_dup = (~df["_sale_id_missing"]) & (~df["_sale_id_invalid"])
    dup_mask = valid_sale_id_for_dup & df["sale_id_num"].duplicated(keep=False)
    dup_sales = cast(
        pd.DataFrame,
        df.loc[dup_mask].copy(),
    ).sort_values(by="sale_id_num")
    logger.debug("[TRANSFORM][sales] duplicated_sale_id=%s", int(dup_mask.sum()))
    if dup_mask.any():
        logger.debug("[TRANSFORM][sales] duplicated sample:\n%s", dup_sales.head(10))

    dup_preview = cast(
        pd.DataFrame,
        df.loc[
            dup_mask,
            [
                "sale_id",
                "sale_id_num",
                "customer_id",
                "customer_id_num",
                "product_id",
                "product_id_num",
                "sale_date",
                "quantity",
                "discount",
                "_score",
            ],
        ].copy(),
    )
    if dup_mask.any():
        logger.debug(
            "[TRANSFORM][sales] duplicates with score sample:\n%s",
            dup_preview.sort_values(
                by=["sale_id_num", "_score"], ascending=[True, False]
            ).head(10),
        )


def _deduplicate_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the best row per valid sale_id while preserving invalid-id rows for rejection."""

    valid_sale_id = (~df["_sale_id_missing"]) & (~df["_sale_id_invalid"])
    df_valid_sale_id = cast(pd.DataFrame, df.loc[valid_sale_id].copy())
    df_invalid_sale_id = cast(pd.DataFrame, df.loc[~valid_sale_id].copy())

    df_sorted = df_valid_sale_id.sort_values(
        by=["sale_id_num", "_score", "sale_date_parsed"],
        ascending=[True, False, False],
        kind="mergesort",
    )
    df_dedup_valid = df_sorted.drop_duplicates(
        subset=["sale_id_num"], keep="first"
    ).copy()
    return pd.concat([df_dedup_valid, df_invalid_sale_id], ignore_index=True)


def _assign_reject_reasons_and_split(
    df: pd.DataFrame,
    valid_customers: set[int],
    valid_products: set[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Assign ordered reject reasons and split rows into clean vs rejected sets."""

    bad_sale_id_missing = df["_sale_id_missing"]
    bad_sale_id_invalid = df["_sale_id_invalid"]
    bad_date = df["sale_date_parsed"].isna()
    bad_cust_missing = df["_cust_id_missing"]
    bad_cust_invalid = df["_cust_id_invalid"]
    bad_prod_missing = df["_prod_id_missing"]
    bad_prod_invalid = df["_prod_id_invalid"]

    bad_qty = (~df["_qty_is_int"]) | (df["quantity_num"] <= 0)
    bad_disc_format = df["_disc_invalid"]
    bad_disc = (df["discount_num"] < 0) | (df["discount_num"] > 100)

    bad_cust_fk = (
        (~df["customer_id_num"].isin(valid_customers))
        & (~bad_cust_missing)
        & (~bad_cust_invalid)
    )
    bad_prod_fk = (
        (~df["product_id_num"].isin(valid_products))
        & (~bad_prod_missing)
        & (~bad_prod_invalid)
    )

    reason_masks = {
        "missing_sale_id": bad_sale_id_missing,
        "invalid_sale_id": bad_sale_id_invalid,
        "missing_customer_id": bad_cust_missing,
        "invalid_customer_id": bad_cust_invalid,
        "missing_product_id": bad_prod_missing,
        "invalid_product_id": bad_prod_invalid,
        "invalid_sale_date": bad_date,
        "unknown_customer_id": bad_cust_fk,
        "unknown_product_id": bad_prod_fk,
        "invalid_quantity": bad_qty,
        "invalid_discount": bad_disc_format,
        "discount_out_of_range": bad_disc,
    }
    reason_rules = [
        (reason, reason_masks[reason]) for reason in REJECT_REASONS_PRIORITY
    ]
    reason_flags = pd.DataFrame(
        {reason: mask.fillna(False).astype(bool) for reason, mask in reason_rules},
        index=df.index,
    )
    df["reject_reasons"] = _build_reject_reasons(reason_flags)

    reject_mask = df["reject_reasons"].notna()
    return (
        cast(pd.DataFrame, df.loc[~reject_mask].copy()),
        cast(pd.DataFrame, df.loc[reject_mask].copy()),
    )


def _finalize_sales_clean(sales_clean: pd.DataFrame) -> pd.DataFrame:
    """Project clean sales rows back to the public contract and drop auxiliaries."""

    sales_clean["sale_date"] = sales_clean["sale_date_parsed"]
    sales_clean["quantity"] = sales_clean["quantity_num"].astype("Int64")
    sales_clean["discount"] = sales_clean["discount_num"]
    sales_clean["customer_id"] = sales_clean["customer_id_num"].astype("int64")
    sales_clean["product_id"] = sales_clean["product_id_num"].astype("int64")
    sales_clean["sale_id"] = sales_clean["sale_id_num"].astype("int64")

    return sales_clean.drop(
        columns=[*AUXILIARY_SALES_COLS, *CLEAN_ONLY_DROP_COLS],
        errors="ignore",
    ).reset_index(drop=True)


def _finalize_sales_rejected(sales_rejected: pd.DataFrame) -> pd.DataFrame:
    """Drop internal auxiliary columns from rejected sales while keeping reject reasons."""

    return sales_rejected.drop(
        columns=list(AUXILIARY_SALES_COLS),
        errors="ignore",
    ).reset_index(drop=True)


def _log_reject_summary(sales_rejected: pd.DataFrame) -> None:
    """Log a deterministic summary of rejected sales grouped by business reason."""

    reject_counts_series = (
        sales_rejected["reject_reasons"]
        .dropna()
        .astype("string")
        .str.split("|")
        .explode()
        .value_counts()
    )
    reject_counts = {
        reason: int(reject_counts_series[reason])
        for reason in REJECT_REASONS_PRIORITY
        if reason in reject_counts_series
    }
    logger.info(
        "[TRANSFORM][sales] rejected_rows=%s by_reason=%s",
        len(sales_rejected),
        reject_counts,
    )


def clean_sales(
    df: pd.DataFrame,
    customers_clean: pd.DataFrame,
    products_clean: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize sales, keep the best duplicate candidate, and split rejected rows."""
    df = df.copy()

    valid_customers = set(customers_clean["customer_id"])
    valid_products = set(products_clean["product_id"])

    df = _parse_sales_fields(df)
    df = _flag_sales_quality(df, valid_customers, valid_products)
    _log_duplicate_sales(df)
    df = _deduplicate_sales(df)

    sales_clean, sales_rejected = _assign_reject_reasons_and_split(
        df,
        valid_customers,
        valid_products,
    )
    _log_reject_summary(sales_rejected)

    return _finalize_sales_clean(sales_clean), _finalize_sales_rejected(sales_rejected)
