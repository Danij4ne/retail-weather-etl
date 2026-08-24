import pandas as pd

from etl.transform_parts.date_parsing import parse_dates


def test_parse_dates_accepts_hyphen_and_slash_year_month_day():
    raw = pd.Series(["2025-01-13", "2025/01/16"])

    parsed = parse_dates(raw, dataset="sales")

    assert parsed.tolist() == [
        pd.Timestamp("2025-01-13"),
        pd.Timestamp("2025-01-16"),
    ]


def test_parse_dates_does_not_depend_on_neighboring_rows():
    raw = pd.Series(["2025-01-13", "2025/01/16", "31/01/2025", "bad_date", "", None])

    parsed = parse_dates(raw, dataset="sales")

    assert parsed.iloc[0] == pd.Timestamp("2025-01-13")
    assert parsed.iloc[1] == pd.Timestamp("2025-01-16")
    assert pd.isna(parsed.iloc[2])
    assert pd.isna(parsed.iloc[3])
    assert pd.isna(parsed.iloc[4])
    assert pd.isna(parsed.iloc[5])


def test_parse_dates_rejects_day_first_even_when_unambiguous():
    raw = pd.Series(["31/01/2025", "01/02/2025"])

    parsed = parse_dates(raw, dataset="customers")

    assert parsed.isna().all()


def test_parse_dates_slash_only_batch_still_parses():
    raw = pd.Series(["2025/01/16", "2025/03/11"])

    parsed = parse_dates(raw, dataset="sales")

    assert parsed.tolist() == [
        pd.Timestamp("2025-01-16"),
        pd.Timestamp("2025-03-11"),
    ]
