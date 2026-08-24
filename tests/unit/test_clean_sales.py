import pandas as pd

from etl.transform import clean_sales
from etl.transform_parts.clean_sales import REJECT_REASONS_PRIORITY


def test_clean_sales_splits_clean_and_rejected(raw_sales_df, cleaned_dimensions):
    customers_clean, products_clean = cleaned_dimensions
    sales_clean, sales_rejected = clean_sales(
        raw_sales_df, customers_clean, products_clean
    )

    assert sales_clean["sale_id"].tolist() == [10]
    assert len(sales_rejected) == 5

    expected_reasons = {
        "invalid_quantity",
        "discount_out_of_range",
        "unknown_customer_id",
        "unknown_product_id",
        "missing_customer_id",
    }
    assert set(sales_rejected["reject_reasons"]) == expected_reasons


def test_clean_sales_casts_numeric_fields(raw_sales_df, cleaned_dimensions):
    customers_clean, products_clean = cleaned_dimensions
    sales_clean, _ = clean_sales(raw_sales_df, customers_clean, products_clean)

    row = sales_clean.iloc[0]
    assert str(sales_clean["quantity"].dtype) == "Int64"
    assert row["quantity"] == 2
    assert row["discount"] == 10
    assert row["customer_id"] == 1
    assert row["product_id"] == 1001


def test_clean_sales_rejects_decimal_quantity(cleaned_dimensions):
    customers_clean, products_clean = cleaned_dimensions
    raw_sales = pd.DataFrame(
        {
            "sale_id": [99],
            "customer_id": [1],
            "product_id": [1001],
            "sale_date": ["2024-03-10"],
            "quantity": [2.5],
            "discount": [10],
        }
    )

    sales_clean, sales_rejected = clean_sales(
        raw_sales, customers_clean, products_clean
    )

    assert sales_clean.empty
    assert sales_rejected["reject_reasons"].tolist() == ["invalid_quantity"]


def test_clean_sales_rejected_output_drops_internal_auxiliary_columns(
    raw_sales_df, cleaned_dimensions
):
    customers_clean, products_clean = cleaned_dimensions
    _, sales_rejected = clean_sales(raw_sales_df, customers_clean, products_clean)

    assert sales_rejected.columns.tolist() == [
        "sale_id",
        "customer_id",
        "product_id",
        "sale_date",
        "quantity",
        "discount",
        "reject_reasons",
    ]


def test_clean_sales_reject_reasons_contract_order(cleaned_dimensions):
    customers_clean, products_clean = cleaned_dimensions
    raw_sales = pd.DataFrame(
        {
            "sale_id": [200],
            "customer_id": [9999],
            "product_id": [1001],
            "sale_date": ["bad_date"],
            "quantity": [0],
            "discount": [200],
        }
    )

    sales_clean, sales_rejected = clean_sales(
        raw_sales, customers_clean, products_clean
    )

    assert sales_clean.empty
    observed_reasons = sales_rejected["reject_reasons"].tolist()
    assert observed_reasons == [
        "invalid_sale_date|unknown_customer_id|invalid_quantity|discount_out_of_range"
    ]
    assert observed_reasons[0].split("|") == [
        reason
        for reason in REJECT_REASONS_PRIORITY
        if reason
        in {
            "invalid_sale_date",
            "unknown_customer_id",
            "invalid_quantity",
            "discount_out_of_range",
        }
    ]


def test_clean_sales_accepts_slash_separated_year_month_day(cleaned_dimensions):
    customers_clean, products_clean = cleaned_dimensions
    raw_sales = pd.DataFrame(
        {
            "sale_id": [300],
            "customer_id": [1],
            "product_id": [1001],
            "sale_date": ["2025/01/16"],
            "quantity": [2],
            "discount": [10],
        }
    )

    sales_clean, sales_rejected = clean_sales(
        raw_sales, customers_clean, products_clean
    )

    assert sales_rejected.empty
    assert sales_clean["sale_date"].tolist() == [pd.Timestamp("2025-01-16")]


def test_clean_sales_rejects_day_first_dates_even_when_unambiguous(
    cleaned_dimensions,
):
    customers_clean, products_clean = cleaned_dimensions
    raw_sales = pd.DataFrame(
        {
            "sale_id": [301],
            "customer_id": [1],
            "product_id": [1001],
            "sale_date": ["31/01/2025"],
            "quantity": [2],
            "discount": [10],
        }
    )

    sales_clean, sales_rejected = clean_sales(
        raw_sales, customers_clean, products_clean
    )

    assert sales_clean.empty
    assert sales_rejected["reject_reasons"].tolist() == ["invalid_sale_date"]


def test_clean_sales_mixed_date_formats_keep_year_month_day_only(
    cleaned_dimensions,
):
    customers_clean, products_clean = cleaned_dimensions
    raw_sales = pd.DataFrame(
        {
            "sale_id": [400, 401, 402, 403],
            "customer_id": [1, 1, 1, 1],
            "product_id": [1001, 1001, 1001, 1001],
            "sale_date": ["2024-03-01", "2025/01/16", "31/01/2025", "bad_date"],
            "quantity": [1, 1, 1, 1],
            "discount": [0, 0, 0, 0],
        }
    )

    sales_clean, sales_rejected = clean_sales(
        raw_sales, customers_clean, products_clean
    )

    assert sales_clean["sale_id"].tolist() == [400, 401]
    assert sales_rejected["sale_id"].tolist() == [402, 403]
    assert sales_rejected["reject_reasons"].tolist() == [
        "invalid_sale_date",
        "invalid_sale_date",
    ]
