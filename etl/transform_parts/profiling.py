from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger("etl.transform")


def _has(df: pd.DataFrame, *columns: str) -> bool:
    """Return whether every named column exists. Profiling must not KeyError."""
    return all(column in df.columns for column in columns)


def profiling(results: dict[str, pd.DataFrame], detail: bool = False) -> None:
    critical_columns = [
        "customer_id",
        "city",
        "signup_date",
        "product_id",
        "price",
        "category",
        "sale_id",
        "sale_date",
        "quantity",
        "discount",
        "date",
    ]

    for name, df in results.items():
        nulls_per_col = df.isnull().sum()
        key_nulls: dict[str, int] = {}
        for col in critical_columns:
            if _has(df, col):
                key_nulls[col] = int(df[col].isnull().sum())

        duplicate_rows = int(df.duplicated().sum())

        key_duplicates = 0
        if name == "customers" and _has(df, "customer_id"):
            key_duplicates = int(df["customer_id"].duplicated().sum())
        elif name == "products" and _has(df, "product_id"):
            key_duplicates = int(df["product_id"].duplicated().sum())
        elif name == "sales" and _has(df, "sale_id"):
            key_duplicates = int(df["sale_id"].duplicated().sum())
        elif name == "weather_daily" and _has(df, "date", "city"):
            key_duplicates = int(df[["date", "city"]].duplicated().sum())

        categorical_cols = df.select_dtypes(include=["object", "string"]).columns
        cardinality_summary: dict[str, int] = {}
        for col in categorical_cols:
            cardinality_summary[col] = int(df[col].nunique())

        violations: dict[str, int] = {}
        if name == "products" and _has(df, "price"):
            price_num = pd.to_numeric(df["price"], errors="coerce")
            violations["price_le_0"] = int((price_num <= 0).sum())
        elif name == "sales":
            if _has(df, "quantity"):
                quantity_num = pd.to_numeric(df["quantity"], errors="coerce")
                violations["quantity_le_0"] = int((quantity_num <= 0).sum())
            if _has(df, "discount"):
                discount_num = pd.to_numeric(df["discount"], errors="coerce")
                violations["discount_out_of_range"] = int(
                    ((discount_num < 0) | (discount_num > 100)).sum()
                )
        elif name == "weather_daily" and _has(df, "precip_mm"):
            precip_num = pd.to_numeric(df["precip_mm"], errors="coerce")
            violations["precip_mm_lt_0"] = int((precip_num < 0).sum())
        logger.info(
            "[PROFILE][%s] rows=%s cols=%s nulls_total=%s duplicated_rows=%s key_duplicates=%s violations=%s",
            name,
            df.shape[0],
            df.shape[1],
            int(nulls_per_col.sum()),
            duplicate_rows,
            key_duplicates,
            violations,
        )

        logger.debug(
            "[PROFILE][%s] nulls_avg_per_col=%.2f",
            name,
            float(nulls_per_col.mean()),
        )
        if key_nulls:
            logger.debug("[PROFILE][%s] key_nulls=%s", name, key_nulls)
        if cardinality_summary:
            logger.debug("[PROFILE][%s] cardinality=%s", name, cardinality_summary)
        logger.debug("[PROFILE][%s] rule_violations=%s", name, violations)

        if detail:
            logger.debug("[PROFILE][%s] columns=%s", name, list(df.columns))
            logger.debug("[PROFILE][%s] dtypes=\n%s", name, df.dtypes)
            logger.debug("[PROFILE][%s] nulls_by_column=\n%s", name, nulls_per_col)
            for col in categorical_cols:
                logger.debug(
                    "[PROFILE][%s] top_values[%s]=\n%s",
                    name,
                    col,
                    df[col].value_counts().head(10),
                )
