"""Schema definitions for ETL dataset contracts."""

from etl.schemas.silver import (
    CUSTOMERS_SILVER_SCHEMA,
    PRODUCTS_SILVER_SCHEMA,
    SALES_SILVER_SCHEMA,
    WEATHER_DAILY_SILVER_SCHEMA,
)

__all__ = [
    "CUSTOMERS_SILVER_SCHEMA",
    "PRODUCTS_SILVER_SCHEMA",
    "SALES_SILVER_SCHEMA",
    "WEATHER_DAILY_SILVER_SCHEMA",
]
