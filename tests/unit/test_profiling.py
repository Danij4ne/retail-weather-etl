import logging

import pandas as pd

from etl.transform_parts.profiling import profiling


def test_profiling_logs_info_summary_and_debug_detail(raw_results, caplog):
    logger_name = "etl.transform"

    with caplog.at_level(logging.INFO, logger=logger_name):
        profiling(raw_results, detail=False)

    info_messages = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.INFO and "[PROFILE]" in record.getMessage()
    ]

    assert any("[PROFILE][customers] rows=" in message for message in info_messages)
    assert any("[PROFILE][products] rows=" in message for message in info_messages)
    assert not any("columns=" in message for message in info_messages)

    caplog.clear()

    with caplog.at_level(logging.DEBUG, logger=logger_name):
        profiling(raw_results, detail=True)

    debug_messages = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.DEBUG and "[PROFILE]" in record.getMessage()
    ]

    assert any("columns=" in message for message in debug_messages)
    assert any("nulls_by_column=" in message for message in debug_messages)


def test_profiling_skips_missing_product_price_without_raising(caplog):
    products = pd.DataFrame(
        {
            "product_id": [1001, 1002],
            "product_name": ["botella", "zapatilla"],
            "category": ["accessories", "footwear"],
        }
    )

    with caplog.at_level(logging.INFO, logger="etl.transform"):
        profiling({"products": products}, detail=False)

    assert any(
        "[PROFILE][products] rows=" in record.getMessage() for record in caplog.records
    )


def test_profiling_skips_missing_customer_id_without_raising(caplog):
    customers = pd.DataFrame(
        {
            "first_name": ["ana", "luis"],
            "city": ["madrid", "barcelona"],
        }
    )

    with caplog.at_level(logging.INFO, logger="etl.transform"):
        profiling({"customers": customers}, detail=False)

    assert any(
        "[PROFILE][customers] rows=" in record.getMessage() for record in caplog.records
    )
