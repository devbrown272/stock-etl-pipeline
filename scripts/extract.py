"""
extract.py
Fetches daily data from the
Alpha Vantage TIME_SERIES_DAILY endpoint.

Free API key: https://www.alphavantage.co/support/#api-key
Rate limit   : 25/day
"""

from __future__ import annotations

import os
import time
import logging

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://www.alphavantage.co/query"
API_KEY  = os.environ["ALPHA_VANTAGE_API_KEY"]   # set in Airflow Variables/Connections

# How many trading days to pull (controls backfill depth)
OUTPUT_SIZE = "compact"   # "compact" = last 100 days; "full" = 20+ years


def fetch_stock_data(ticker: str, retries: int = 3) -> dict:
    """
    Call Alpha Vantage TIME_SERIES_DAILY and return the raw time-series dict.

    Parameters
    ----------
    ticker  : stock symbol, e.g. "AAPL"
    retries : number of retry attempts on transient HTTP errors

    Returns
    -------
    dict  {date_str: {"1. open": ..., "2. high": ..., ...}}
    """
    params = {
        "function":   "TIME_SERIES_DAILY",
        "symbol":     ticker,
        "outputsize": OUTPUT_SIZE,
        "apikey":     API_KEY,
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()

            # Alpha Vantage will give error for more than 200 pulls
            if "Error Message" in payload:
                raise ValueError(f"Alpha Vantage error for {ticker}: {payload['Error Message']}")
            if "Note" in payload:
                # API rate-limit warning
                logger.warning("Rate-limit note for %s: %s", ticker, payload["Note"])

            time_series = payload.get("Time Series (Daily)")
            if not time_series:
                raise KeyError(f"'Time Series (Daily)' missing in response for {ticker}")

            return time_series

        except (requests.RequestException, KeyError, ValueError) as exc:
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, retries, ticker, exc)
            if attempt == retries:
                raise
            time.sleep(5 * attempt)   # back off girlie
