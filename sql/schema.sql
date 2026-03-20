
-- schema.sql
-- Usage: mysql -u root -p < sql/schema.sql

CREATE DATABASE IF NOT EXISTS stock_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE stock_db;

-- Main fact table: one row per (ticker, trading day)

CREATE TABLE IF NOT EXISTS stock_prices (
    id           BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    ticker       VARCHAR(10)      NOT NULL COMMENT 'Stock symbol e.g. AAPL',
    trade_date   DATE             NOT NULL COMMENT 'Trading day (YYYY-MM-DD)',

    -- OHLCV
    open         DECIMAL(12, 4)   NOT NULL,
    high         DECIMAL(12, 4)   NOT NULL,
    low          DECIMAL(12, 4)   NOT NULL,
    close        DECIMAL(12, 4)   NOT NULL,
    volume       BIGINT UNSIGNED  NOT NULL,

    -- Derived metrics
    daily_return DECIMAL(8, 4)    NULL     COMMENT '% change vs previous close',
    price_range  DECIMAL(12, 4)   NOT NULL COMMENT 'Intraday high - low spread',

    -- Data quality & audit
    data_quality TINYINT(1)       NOT NULL DEFAULT 1 COMMENT '1=OK, 0=suspect',
    ingested_at  DATETIME         NOT NULL COMMENT 'UTC timestamp of ETL run',

    PRIMARY KEY (id),
    UNIQUE KEY uq_ticker_date (ticker, trade_date),
    INDEX idx_ticker       (ticker),
    INDEX idx_trade_date   (trade_date),
    INDEX idx_daily_return (daily_return)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COMMENT='Daily OHLCV stock prices loaded by Airflow ETL pipeline';

--quick analysis
CREATE OR REPLACE VIEW v_stock_summary AS
SELECT
    ticker,
    MIN(trade_date)                         AS earliest_date,
    MAX(trade_date)                         AS latest_date,
    COUNT(*)                                AS trading_days,
    ROUND(AVG(close), 2)                    AS avg_close,
    ROUND(MIN(low),   2)                    AS period_low,
    ROUND(MAX(high),  2)                    AS period_high,
    ROUND(AVG(volume) / 1e6, 2)             AS avg_volume_M,
    ROUND(AVG(daily_return), 4)             AS avg_daily_return_pct,
    SUM(data_quality = 0)                   AS suspect_rows
FROM stock_prices
GROUP BY ticker
ORDER BY ticker;
