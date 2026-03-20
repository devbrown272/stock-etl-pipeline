"""
transform.py
Cleans and enriches the raw Alpha Vantage payload.

Transformations applied:
1. Rename verbose Alpha Vantage keys → clean column names
2. Cast all numeric fields to float/int
3. Calculate daily_return  (% change from previous close)
4. Calculate price_range   (high - low)
5. Add data_quality flag   (flags days with zero volume or missing fields)
6. Attach the ticker symbol and an ingestion timestamp
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Alpha Vantage key for each column
FIELD_MAP = {
    "1. open":   "open",
    "2. high":   "high",
    "3. low":    "low",
    "4. close":  "close",
    "5. volume": "volume",
}


def transform_stock_data(ticker: str, raw: dict[str, dict]) -> list[dict[str, Any]]:
    """
    Convert a raw Alpha data into readable

    ticker : stock symbol ("AAPL", etc.)
    raw    : {date_str: {"1. open": "182.50", ...}, ...}

    Returns
    if not raw:
        logger.warning("No data to transform for %s", ticker)
        return []

    # Sort dates ascending
    sorted_dates = sorted(raw.keys())
    records: list[dict] = []
    prev_close: float | None = None

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for date_str in sorted_dates:
        day = raw[date_str]

        try:
            open_p  = float(day["1. open"])
            high_p  = float(day["2. high"])
            low_p   = float(day["3. low"])
            close_p = float(day["4. close"])
            volume  = int(day["5. volume"])
        except (KeyError, ValueError) as exc:
            logger.warning("Skipping malformed record %s %s: %s", ticker, date_str, exc)
            continue

        # Derived metrics
        daily_return = (
            round((close_p - prev_close) / prev_close * 100, 4)
            if prev_close is not None
            else None
        )
        price_range = round(high_p - low_p, 4)

        # Data quality flag
        quality_ok = volume > 0 and all(
            v > 0 for v in [open_p, high_p, low_p, close_p]
        )

        records.append(
            {
                "ticker":        ticker,
                "trade_date":    date_str,
                "open":          open_p,
                "high":          high_p,
                "low":           low_p,
                "close":         close_p,
                "volume":        volume,
                "daily_return":  daily_return,   # % change vs previous day
                "price_range":   price_range,    # intraday spread
                "data_quality":  1 if quality_ok else 0,
                "ingested_at":   ingested_at,
            }
        )

        prev_close = close_p

    logger.info(
        "transform_stock_data: %s → %d records (%d skipped)",
        ticker,
        len(records),
        len(sorted_dates) - len(records),
    )
    return records
