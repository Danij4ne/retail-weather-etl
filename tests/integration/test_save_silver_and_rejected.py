import pandas as pd
import pytest

from etl.transform import save_silver_and_rejected


@pytest.mark.integration
def test_save_silver_and_rejected_writes_expected_files(
    tmp_path, monkeypatch, transformed_outputs
):
    cleaned, rejected = transformed_outputs
    monkeypatch.chdir(tmp_path)

    save_silver_and_rejected(cleaned, rejected)

    silver_dir = tmp_path / "data" / "processed" / "silver"
    rejected_dir = tmp_path / "data" / "processed" / "rejected"

    assert (silver_dir / "customers_clean.parquet").exists()
    assert (silver_dir / "products_clean.parquet").exists()
    assert (silver_dir / "sales_clean.parquet").exists()
    assert (silver_dir / "weather_daily_clean.parquet").exists()
    assert (rejected_dir / "sales_rejected.csv").exists()

    customers_saved = pd.read_parquet(silver_dir / "customers_clean.parquet")
    rejected_saved = pd.read_csv(rejected_dir / "sales_rejected.csv")

    assert len(customers_saved) == len(cleaned["customers"])
    assert len(rejected_saved) == len(rejected["sales"])


@pytest.mark.integration
def test_save_silver_and_rejected_enforces_output_column_contract(
    tmp_path, monkeypatch, transformed_outputs
):
    cleaned, rejected = transformed_outputs
    monkeypatch.chdir(tmp_path)

    cleaned = {name: df.copy() for name, df in cleaned.items()}
    rejected = {name: df.copy() for name, df in rejected.items()}
    cleaned["sales"]["debug_helper"] = "x"
    rejected["sales"]["debug_helper"] = "x"

    save_silver_and_rejected(cleaned, rejected)

    silver_dir = tmp_path / "data" / "processed" / "silver"
    rejected_dir = tmp_path / "data" / "processed" / "rejected"

    sales_saved = pd.read_parquet(silver_dir / "sales_clean.parquet")
    rejected_saved = pd.read_csv(rejected_dir / "sales_rejected.csv")

    assert sales_saved.columns.tolist() == [
        "sale_id",
        "customer_id",
        "product_id",
        "sale_date",
        "quantity",
        "discount",
    ]
    assert rejected_saved.columns.tolist() == [
        "sale_id",
        "customer_id",
        "product_id",
        "sale_date",
        "quantity",
        "discount",
        "reject_reasons",
    ]
