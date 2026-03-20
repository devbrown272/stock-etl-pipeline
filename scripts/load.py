"""
load.py
Upserts transformed stock records into MySQL using mysql-connector-python.

Strategy: INSERT ... ON DUPLICATE KEY UPDATE
  - Primary key is (ticker, trade_date)
  - Only certain columns (prices, volume, derived metrics) are updated on conflict.
"""

from __future__ import annotations

import logging
import os

import mysql.connector
from mysql.connector import Error as MySQLError

logger = logging.getLogger(__name__)

# Connection config (pulled from Airflow Variables)
DB_CONFIG = {
    "host":     os.environ.get("MYSQL_HOST",     "localhost"),
    "port":     int(os.environ.get("MYSQL_PORT", "3306")),
    "user":     os.environ.get("MYSQL_USER",     "airflow"),
    "password": os.environ.get("MYSQL_PASSWORD", ""),
    "database": os.environ.get("MYSQL_DATABASE", "stock_db"),
}

UPSERT_SQL = """
INSERT INTO stock_prices (
    ticker, trade_date, open, high, low, close, volume,
    daily_return, price_range, data_quality, ingested_at
)
VALUES (
    %(ticker)s, %(trade_date)s, %(open)s, %(high)s, %(low)s, %(close)s,
    %(volume)s, %(daily_return)s, %(price_range)s, %(data_quality)s, %(ingested_at)s
)
ON DUPLICATE KEY UPDATE
    open          = VALUES(open),
    high          = VALUES(high),
    low           = VALUES(low),
    close         = VALUES(close),
    volume        = VALUES(volume),
    daily_return  = VALUES(daily_return),
    price_range   = VALUES(price_range),
    data_quality  = VALUES(data_quality),
    ingested_at   = VALUES(ingested_at);
"""


def load_to_mysql(records: list[dict], batch_size: int = 100) -> None:
    """
    Adds list to table.

    Parameters
    ----------
    records    : output of transform_stock_data()
    batch_size : number of rows per executemany() call
    """
    if not records:
        logger.info("No records to load — skipping.")
        return

    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Batch to avoid server strain
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            cursor.executemany(UPSERT_SQL, batch)
            connection.commit()
            logger.info("Upserted batch %d–%d", i + 1, i + len(batch))

        logger.info("load_to_mysql: %d total records upserted.", len(records))

    except MySQLError as exc:
        logger.error("MySQL error during load: %s", exc)
        if connection:
            connection.rollback()
        raise

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
