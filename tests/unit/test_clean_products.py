import pandas as pd
import pytest

from etl.transform import clean_products


def test_clean_products_deduplicates_and_normalizes(raw_products_df):
    cleaned = clean_products(raw_products_df)

    assert cleaned["product_id"].tolist() == [1001, 1002]
    assert cleaned["product_id"].duplicated().sum() == 0

    product_1001 = cleaned.loc[cleaned["product_id"] == 1001].iloc[0]
    assert product_1001["product_name"] == "Botella"
    assert product_1001["category"] == "Accessories"
    assert product_1001["price"] == pytest.approx(15.5)
    assert bool(product_1001["is_price_valid"]) is True

    product_1002 = cleaned.loc[cleaned["product_id"] == 1002].iloc[0]
    assert pd.isna(product_1002["price"])
    assert bool(product_1002["is_price_valid"]) is False


def test_clean_products_leet_is_conservative():
    raw = pd.DataFrame(
        {
            "product_id": [2001, 2002, 2003],
            "product_name": ["Trail Shoes 3.0", "B0tella", "Pack 10L"],
            "category": ["accessories", "accessories", "accessories"],
            "price": ["10", "10", "10"],
        }
    )

    cleaned = clean_products(
        raw,
        category_map={"accessories": "accessories"},
        leet_enabled=True,
        leet_map={"0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t"},
    )

    assert (
        cleaned.loc[cleaned["product_id"] == 2001, "product_name"].iloc[0]
        == "Trail Shoes 3.0"
    )
    assert (
        cleaned.loc[cleaned["product_id"] == 2002, "product_name"].iloc[0] == "Botella"
    )
    assert (
        cleaned.loc[cleaned["product_id"] == 2003, "product_name"].iloc[0] == "Pack 10L"
    )


def test_clean_products_can_disable_leet():
    raw = pd.DataFrame(
        {
            "product_id": [3001],
            "product_name": ["B0tella"],
            "category": ["accessories"],
            "price": ["10"],
        }
    )

    cleaned = clean_products(
        raw,
        category_map={"accessories": "accessories"},
        leet_enabled=False,
        leet_map={"0": "o"},
    )

    assert (
        cleaned.loc[cleaned["product_id"] == 3001, "product_name"].iloc[0] == "B0Tella"
    )


def test_clean_products_preserves_unicode_and_accents_in_names():
    raw = pd.DataFrame(
        {
            "product_id": [3501],
            "product_name": ["  bótella térmica  "],
            "category": ["accessories"],
            "price": ["10"],
        }
    )

    cleaned = clean_products(
        raw,
        category_map={"accessories": "accessories"},
    )

    product = cleaned.loc[cleaned["product_id"] == 3501].iloc[0]
    assert product["product_name"] == "Bótella Térmica"
    assert product["category"] == "Accessories"


def test_clean_products_normalizes_unknown_and_unmapped_categories():
    raw = pd.DataFrame(
        {
            "product_id": [4001, 4002, 4003, 4004],
            "product_name": ["x", "y", "z", "w"],
            "category": ["Misc", "N/A", "totally_new_category", None],
            "price": ["10", "10", "10", "10"],
        }
    )

    cleaned = clean_products(
        raw,
        category_map={"accessories": "accessories"},
    )

    assert set(cleaned["category"]) == {"Unknown"}


def test_clean_products_price_parsing_handles_simple_decimal_formats_only():
    raw = pd.DataFrame(
        {
            "product_id": [5001, 5002, 5003, 5004],
            "product_name": ["a", "b", "c", "d"],
            "category": ["accessories"] * 4,
            "price": ["1234,56", "1234.56", "1.234,56", "1,234.56"],
        }
    )

    cleaned = clean_products(
        raw,
        category_map={"accessories": "accessories"},
    )

    product_5001 = cleaned.loc[cleaned["product_id"] == 5001].iloc[0]
    assert float(product_5001["price"]) == pytest.approx(1234.56)
    assert bool(product_5001["is_price_valid"]) is True

    product_5002 = cleaned.loc[cleaned["product_id"] == 5002].iloc[0]
    assert float(product_5002["price"]) == pytest.approx(1234.56)
    assert bool(product_5002["is_price_valid"]) is True

    product_5003 = cleaned.loc[cleaned["product_id"] == 5003].iloc[0]
    assert pd.isna(product_5003["price"])
    assert bool(product_5003["is_price_valid"]) is False

    product_5004 = cleaned.loc[cleaned["product_id"] == 5004].iloc[0]
    assert pd.isna(product_5004["price"])
    assert bool(product_5004["is_price_valid"]) is False
