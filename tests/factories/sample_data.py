import pandas as pd

DataFrameDict = dict[str, pd.DataFrame]


# Tests clean_customers:
# - Deduplication by customer_id
# - Normalization of city/names
# - signup_date parsing and null handling
# Built this way to cover minimal dirty cases that trigger those rules.
def make_raw_customers_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "customer_id": [1, 1, 2],
            "first_name": [" ana ", "", "luis"],
            "last_name": [None, "lopez", " garcia"],
            "city": ["madríd", "", "bar celona"],
            "signup_date": ["2024-01-01", "2024-01-02", "2024-02-01"],
        }
    )


# Tests clean_products:
# - Deduplication by product_id
# - product_name/category normalization (including typos/leet)
# - price cleanup/parsing, with invalidation for prices <= 0
# Designed with a few rows to cover critical business branches.
def make_raw_products_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_id": [1001, 1001, 1002],
            "product_name": ["B0tella  ", None, "zapatilla"],
            "category": [" accesories ", None, "footw3ar"],
            "price": ["$15,50", "-1", "-5"],
        }
    )


# Tests clean_sales:
# - Deduplication by sale_id
# - Rejection rules (invalid quantity/discount)
# - Referential integrity (unknown or null customer_id/product_id)
# Built to produce 1 clean row plus multiple distinct reject_reasons values.
def make_raw_sales_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sale_id": [10, 10, 11, 12, 13, 14, 15],
            "customer_id": [1, 1, 1, 1, 99, 1, None],
            "product_id": [1001, 1001, 1001, 1001, 1001, 9999, 1001],
            "sale_date": [
                "2024-03-01",
                "bad_date",
                "2024-03-02",
                "2024-03-03",
                "2024-03-04",
                "2024-03-05",
                "2024-03-06",
            ],
            "quantity": [2, 0, 0, 1, 1, 1, 1],
            "discount": [10, 200, 10, 150, 5, 5, None],
        }
    )


# Tests clean_weather_daily:
# - Uniqueness by (date, city)
# - city normalization
# - Numeric casting and weather_code casting
# - Business rule: negative precip_mm -> NA
# Includes duplicates and string-typed numbers to force real transformations.
def make_raw_weather_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2024-03-01", "2024-03-01", "2024-03-02"],
            "city": [" madrid ", "Madrid", "barcelona"],
            "temp_c": ["12.0", "13.0", "18.0"],
            "precip_mm": ["-1", "0.5", "1.0"],
            "precip_hours": ["1", "0", "2"],
            "weather_code": ["1", "2", "3"],
        }
    )


# Tests post_clean_checks in a healthy scenario:
# - No PK duplicates or critical nulls
# - Valid sales FKs against dimensions
# - Clean dtypes expected by validations
# Serves as the baseline test case that should pass without exceptions.
def make_valid_cleaned_for_checks() -> tuple[DataFrameDict, DataFrameDict]:
    cleaned = {
        "customers": pd.DataFrame(
            {
                "customer_id": [1, 2],
                "first_name": ["Ana", "Luis"],
                "last_name": ["Lopez", "Garcia"],
                "city": ["Madrid", "Barcelona"],
                "signup_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            }
        ),
        "products": pd.DataFrame(
            {
                "product_id": [1001, 1002],
                "product_name": ["Botella", "Zapatilla"],
                "category": ["Accessories", "Footwear"],
                "price": [15.5, 20.0],
                "is_price_valid": [True, True],
            }
        ),
        "sales": pd.DataFrame(
            {
                "sale_id": [10, 11],
                "customer_id": [1, 2],
                "product_id": [1001, 1002],
                "sale_date": pd.to_datetime(["2024-03-01", "2024-03-02"]),
                "quantity": pd.Series([2, 1], dtype="Int64"),
                "discount": [10.0, 0.0],
            }
        ),
        "weather_daily": pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-03-01", "2024-03-02"]),
                "city": ["Madrid", "Barcelona"],
                "temp_c": [12.0, 18.0],
                "precip_mm": [0.0, 1.0],
                "precip_hours": [0.0, 2.0],
                "weather_code": pd.Series([1, 2], dtype="Int64"),
            }
        ),
    }
    rejected = {
        "sales": pd.DataFrame(
            {
                "sale_id": pd.Series([], dtype="int64"),
                "customer_id": pd.Series([], dtype="float64"),
                "product_id": pd.Series([], dtype="int64"),
                "sale_date": pd.Series([], dtype="object"),
                "quantity": pd.Series([], dtype="int64"),
                "discount": pd.Series([], dtype="float64"),
                "reject_reasons": pd.Series([], dtype="object"),
            }
        )
    }
    return cleaned, rejected


# Tests transform() as orchestrator:
# - Provides the full input dict with the 4 sources
# - Reuses raw factories to keep consistency across tests
# Used to validate output structure and optional step execution.
def make_raw_results() -> DataFrameDict:
    return {
        "customers": make_raw_customers_df(),
        "products": make_raw_products_df(),
        "sales": make_raw_sales_df(),
        "weather_daily": make_raw_weather_df(),
    }
