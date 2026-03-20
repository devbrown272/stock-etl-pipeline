"""
stock_etl_dag.py
────────────────
Daily ETL pipeline: Alpha Vantage API then transform then MySQL

Schedule: weekday 6:00 PM UTC
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from scripts.extract import fetch_stock_data
from scripts.transform import transform_stock_data
from scripts.load import load_to_mysql

logger = logging.getLogger(__name__)

# DAG default args
DEFAULT_ARGS = {
    "owner": "portfolio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# StoNcks
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]


# Task FUNctions

def extract_task(ticker: str, **context) -> dict:
    """Pull daily OHLCV data from Alpha Vantage for a single ticker."""
    logger.info("Extracting data for %s", ticker)
    raw = fetch_stock_data(ticker)
    # Push raw to xcom to push
    context["ti"].xcom_push(key=f"raw_{ticker}", value=raw)
    logger.info("Extracted %d records for %s", len(raw), ticker)
    return raw


def transform_task(ticker: str, **context) -> list[dict]:
    """Clean and enrich the raw data"""
    raw = context["ti"].xcom_pull(key=f"raw_{ticker}")
    logger.info("Transforming data for %s", ticker)
    records = transform_stock_data(ticker, raw)
    context["ti"].xcom_push(key=f"transformed_{ticker}", value=records)
    logger.info("Transformed %d records for %s", len(records), ticker)
    return records


def load_task(ticker: str, **context) -> None:
    """Upsert transformed records into MySQL."""
    records = context["ti"].xcom_pull(key=f"transformed_{ticker}")
    logger.info("Loading %d records for %s into MySQL", len(records), ticker)
    load_to_mysql(records)
    logger.info("Load complete for %s", ticker)


# DAG definition
with DAG(
    dag_id="stock_market_etl",
    default_args=DEFAULT_ARGS,
    description="Daily stock ETL: Alpha Vantage → transform → MySQL",
    schedule="0 18 * * 1-5",          # Weekdays at 18:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "etl", "stocks"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    for ticker in TICKERS:
        extract   = PythonOperator(
            task_id=f"extract_{ticker}",
            python_callable=extract_task,
            op_kwargs={"ticker": ticker},
        )
        transform = PythonOperator(
            task_id=f"transform_{ticker}",
            python_callable=transform_task,
            op_kwargs={"ticker": ticker},
        )
        load = PythonOperator(
            task_id=f"load_{ticker}",
            python_callable=load_task,
            op_kwargs={"ticker": ticker},
        )

        # (runs in parallel per ticker)
        start >> extract >> transform >> load >> end
