import pandas as pd

from etl.transform import clean_customers


def test_clean_customers_deduplicates_and_normalizes(raw_customers_df):
    cleaned = clean_customers(raw_customers_df)

    assert cleaned["customer_id"].tolist() == [1, 2]
    assert cleaned["customer_id"].duplicated().sum() == 0

    customer_1 = cleaned.loc[cleaned["customer_id"] == 1].iloc[0]
    assert customer_1["city"] == "Madrid"
    assert customer_1["first_name"] == "Ana"
    assert pd.isna(customer_1["last_name"])

    assert str(cleaned["signup_date"].dtype).startswith("datetime64")


def test_clean_customers_keeps_first_row_on_full_tie():
    raw = pd.DataFrame(
        {
            "customer_id": [7, 7],
            "first_name": ["ana", "eva"],
            "last_name": ["lopez", "garcia"],
            "city": ["madrid", "barcelona"],
            "signup_date": ["2024-01-01", "2024-01-01"],
        }
    )

    cleaned = clean_customers(
        raw,
        city_map={"madrid": "madrid", "barcelona": "barcelona"},
    )

    customer = cleaned.iloc[0]
    assert cleaned["customer_id"].tolist() == [7]
    assert customer["first_name"] == "Ana"
    assert customer["last_name"] == "Lopez"
    assert customer["city"] == "Madrid"


def test_clean_customers_parses_slash_separated_signup_date():
    raw = pd.DataFrame(
        {
            "customer_id": [8],
            "first_name": ["ana"],
            "last_name": ["lopez"],
            "city": ["madrid"],
            "signup_date": ["2024/11/18"],
        }
    )

    cleaned = clean_customers(raw, city_map={"madrid": "madrid"})

    assert cleaned["signup_date"].tolist() == [pd.Timestamp("2024-11-18")]


def test_clean_customers_nulls_day_first_signup_date():
    raw = pd.DataFrame(
        {
            "customer_id": [9],
            "first_name": ["ana"],
            "last_name": ["lopez"],
            "city": ["madrid"],
            "signup_date": ["31/01/2025"],
        }
    )

    cleaned = clean_customers(raw, city_map={"madrid": "madrid"})

    assert len(cleaned) == 1
    assert pd.isna(cleaned.iloc[0]["signup_date"])
