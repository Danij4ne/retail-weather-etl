import pytest

from etl.transform import clean_customers, clean_products, transform
from tests.factories.sample_data import (
    make_raw_customers_df,
    make_raw_products_df,
    make_raw_results,
    make_raw_sales_df,
    make_raw_weather_df,
    make_valid_cleaned_for_checks,
)


@pytest.fixture
def raw_customers_df():
    return make_raw_customers_df()


@pytest.fixture
def raw_products_df():
    return make_raw_products_df()


@pytest.fixture
def raw_sales_df():
    return make_raw_sales_df()


@pytest.fixture
def raw_weather_df():
    return make_raw_weather_df()


@pytest.fixture
def raw_results():
    return make_raw_results()


@pytest.fixture
def cleaned_dimensions(raw_customers_df, raw_products_df):
    customers_clean = clean_customers(raw_customers_df)
    products_clean = clean_products(raw_products_df)
    return customers_clean, products_clean


@pytest.fixture
def transformed_outputs(raw_results):
    return transform(raw_results, run_checks=False, save_outputs=False)


@pytest.fixture
def valid_cleaned_and_rejected_for_checks():
    return make_valid_cleaned_for_checks()
