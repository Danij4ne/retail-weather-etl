import pandas as pd

from etl.transform import clean_weather_daily


def test_clean_weather_daily_normalizes_and_deduplicates(raw_weather_df):
    cleaned = clean_weather_daily(raw_weather_df)

    assert len(cleaned) == 2
    assert set(cleaned["city"]) == {"Madrid", "Barcelona"}
    assert cleaned[["date", "city"]].duplicated().sum() == 0

    madrid_row = cleaned.loc[cleaned["city"] == "Madrid"].iloc[0]
    assert madrid_row["temp_c"] == 13.0
    assert madrid_row["precip_mm"] == 0.5
    assert madrid_row["weather_code"] == 2
    assert str(cleaned["date"].dtype).startswith("datetime64")
    assert str(cleaned["weather_code"].dtype) == "Int64"


def test_clean_weather_daily_prefers_most_complete_duplicate_row():
    raw_weather = pd.DataFrame(
        {
            "date": ["2024-03-01", "2024-03-01"],
            "city": ["madrid", "madrid"],
            "temp_c": [12.0, 13.0],
            "precip_mm": [pd.NA, 0.5],
            "precip_hours": [pd.NA, 0.0],
            "weather_code": [1, 2],
        }
    )

    cleaned = clean_weather_daily(raw_weather)

    assert len(cleaned) == 1
    madrid_row = cleaned.iloc[0]
    assert madrid_row["temp_c"] == 13.0
    assert madrid_row["precip_mm"] == 0.5
    assert madrid_row["precip_hours"] == 0.0
    assert madrid_row["weather_code"] == 2


def test_clean_weather_daily_parses_slash_separated_dates():
    raw_weather = pd.DataFrame(
        {
            "date": ["2024/03/01"],
            "city": ["madrid"],
            "temp_c": [12.0],
            "precip_mm": [0.5],
            "precip_hours": [0.0],
            "weather_code": [1],
        }
    )

    cleaned = clean_weather_daily(raw_weather)

    assert cleaned["date"].tolist() == [pd.Timestamp("2024-03-01")]
