"""Explicit year-month-day parsing for silver date columns.

Pandas inference without ``format`` is batch-dependent: a ``YYYY/MM/DD`` row
can parse or fail depending on which other strings share the Series. This
module parses in a fixed cascade so each value has a stable destination.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger("etl.transform")

# Contract: year-month-day only. Hyphen first (canonical ISO), then slash.
DATE_FORMATS: tuple[tuple[str, str], ...] = (
    ("%Y-%m-%d", "ymd_hyphen"),
    ("%Y/%m/%d", "ymd_slash"),
)


def parse_dates(series: pd.Series, *, dataset: str) -> pd.Series:
    """Parse a raw date series with explicit formats and return datetime64[ns].

    Unrecognized values become NaT. Callers decide whether that means a
    rejected row (sales) or a nullable field (customer signup).
    """
    raw = series.astype("string").str.strip()
    result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    still_open = raw.notna() & (raw != "")

    counts = {label: 0 for _, label in DATE_FORMATS}
    for fmt, label in DATE_FORMATS:
        if not bool(still_open.any()):
            break
        trial = pd.to_datetime(raw.where(still_open), format=fmt, errors="coerce")
        newly = still_open & trial.notna()
        result = result.mask(newly, trial)
        counts[label] = int(newly.sum())
        still_open = still_open & ~newly

    logger.info(
        "[TRANSFORM][%s] date_parse ymd_hyphen=%s ymd_slash=%s unparsed=%s",
        dataset,
        counts["ymd_hyphen"],
        counts["ymd_slash"],
        int(still_open.sum()),
    )
    return result
