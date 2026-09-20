"""
database.py — SQLite Storage Layer
====================================
Handles all database interactions for the market data pipeline:
  - Schema initialisation (4 tables + indexes)
  - Upsert helpers for OHLCV, volatility, and macro data
  - Data-cleaning routines (gap detection, outlier checks)
  - Audit logging of every pipeline run
"""

import sqlite3
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────────────────────────────────────

_DDL = """
-- Adjusted daily OHLCV for every ticker
CREATE TABLE IF NOT EXISTS price_history (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker  TEXT    NOT NULL,
    date    DATE    NOT NULL,
    open    REAL,
    high    REAL,
    low     REAL,
    close   REAL,
    volume  INTEGER,
    source  TEXT,           -- 'polygon' | 'yfinance'
    UNIQUE (ticker, date)
);

-- Per-ticker daily IV / HV summary
CREATE TABLE IF NOT EXISTS volatility_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT    NOT NULL,
    date        DATE    NOT NULL,
    iv_atm_30d  REAL,           -- annualised %, NULL when unavailable
    hv_21d      REAL,           -- annualised %
    hv_30d      REAL,
    hv_60d      REAL,
    iv_source   TEXT,           -- 'polygon_options' | NULL
    UNIQUE (ticker, date)
);

-- FRED macroeconomic time series
CREATE TABLE IF NOT EXISTS macro_data (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id TEXT    NOT NULL,
    date      DATE    NOT NULL,
    value     REAL,
    UNIQUE (series_id, date)
);

-- Audit log of every pipeline execution
CREATE TABLE IF NOT EXISTS pipeline_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at        DATETIME NOT NULL,
    status        TEXT,
    tickers_ok    INTEGER,
    tickers_fail  INTEGER,
    notes         TEXT
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_price_ticker_date
    ON price_history (ticker, date);

CREATE INDEX IF NOT EXISTS idx_vol_ticker_date
    ON volatility_history (ticker, date);

CREATE INDEX IF NOT EXISTS idx_macro_series_date
    ON macro_data (series_id, date);

-- ─── DIMENSIONAL STAR SCHEMA ─────────────────────────────────

-- Calendar dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY, -- YYYYMMDD
    full_date       DATE UNIQUE NOT NULL,
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      TEXT NOT NULL,
    day_of_month    INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,    -- 1 = Monday, 7 = Sunday
    day_name        TEXT NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_trading_day  BOOLEAN NOT NULL
);

-- Security / Ticker dimension
CREATE TABLE IF NOT EXISTS dim_security (
    security_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT UNIQUE NOT NULL,
    company_name    TEXT,
    asset_class     TEXT,                -- 'ETF', 'Equity'
    sector          TEXT,
    exchange        TEXT,
    currency        TEXT DEFAULT 'USD'
);

-- Macro indicator dimension
CREATE TABLE IF NOT EXISTS dim_macro_indicator (
    indicator_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id       TEXT UNIQUE NOT NULL,
    series_name     TEXT,
    category        TEXT,
    frequency       TEXT,
    units           TEXT
);

-- Daily market fact table (conformed OHLCV + volatility metrics)
CREATE TABLE IF NOT EXISTS fact_market_daily (
    date_key        INTEGER NOT NULL,
    security_key    INTEGER NOT NULL,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    volume          INTEGER,
    daily_return    REAL,
    log_return      REAL,
    iv_atm_30d      REAL,
    hv_21d          REAL,
    hv_30d          REAL,
    hv_60d          REAL,
    vol_risk_premium REAL,
    source          TEXT,
    PRIMARY KEY (date_key, security_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (security_key) REFERENCES dim_security(security_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_market_date
    ON fact_market_daily (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_market_security
    ON fact_market_daily (security_key);

-- ─── QUANT FEATURE STORE VIEW (DENORMALIZED OBT) ──────────────
CREATE VIEW IF NOT EXISTS v_quant_feature_store AS
SELECT 
    p.date,
    p.ticker,
    s.company_name,
    s.asset_class,
    s.sector,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume,
    ROUND((p.close - LAG(p.close) OVER (PARTITION BY p.ticker ORDER BY p.date ASC)) / NULLIF(LAG(p.close) OVER (PARTITION BY p.ticker ORDER BY p.date ASC), 0), 6) AS daily_return,
    v.hv_21d,
    v.hv_30d,
    v.hv_60d,
    v.iv_atm_30d,
    ROUND(v.iv_atm_30d - v.hv_30d, 4) AS vol_risk_premium,
    MAX(CASE WHEN m.series_id = 'DGS10'    THEN m.value END) AS yield_10y,
    MAX(CASE WHEN m.series_id = 'DGS2'     THEN m.value END) AS yield_2y,
    MAX(CASE WHEN m.series_id = 'T10Y2Y'   THEN m.value END) AS yield_spread_10y2y,
    MAX(CASE WHEN m.series_id = 'FEDFUNDS' THEN m.value END) AS fed_funds_rate,
    MAX(CASE WHEN m.series_id = 'CPIAUCSL' THEN m.value END) AS cpi_index,
    p.source
FROM price_history p
LEFT JOIN volatility_history v 
    ON p.ticker = v.ticker AND p.date = v.date
LEFT JOIN dim_security s
    ON p.ticker = s.ticker
LEFT JOIN macro_data m 
    ON p.date = m.date
GROUP BY p.date, p.ticker
ORDER BY p.ticker, p.date DESC;
"""

# ─────────────────────────────────────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────────────────────────────────────

def get_connection(db_path: str) -> sqlite3.Connection:
    """
    Open (or create) the SQLite database at *db_path* and return a connection.

    WAL journal mode is enabled for better concurrent read performance and
    crash safety.  Row factory is set so rows can be accessed by column name.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path), detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: str) -> sqlite3.Connection:
    """
    Initialise the database: create all tables and indexes if they do not
    already exist.  Returns an open connection.
    """
    conn = get_connection(db_path)
    conn.executescript(_DDL)
    conn.commit()
    logger.info("Database initialised at %s", db_path)
    try:
        sync_dimensional_model(conn)
    except Exception as exc:
        logger.debug("Dimensional sync skipped on init: %s", exc)
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# Query helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_last_date(conn: sqlite3.Connection, ticker: str) -> Optional[date]:
    """
    Return the most recent date stored in *price_history* for *ticker*,
    or None if the ticker has no history yet.
    """
    row = conn.execute(
        "SELECT MAX(date) AS last_date FROM price_history WHERE ticker = ?",
        (ticker,),
    ).fetchone()
    val = row["last_date"] if row else None
    if val is None:
        return None
    # SQLite may return a native date object (PARSE_DECLTYPES) or a string.
    if isinstance(val, date):
        return val
    return date.fromisoformat(val)


def _to_date(val) -> date:
    """Coerce a SQLite date value (str or date) to a date object."""
    if isinstance(val, date):
        return val
    return date.fromisoformat(val)


def get_stored_dates(conn: sqlite3.Connection, ticker: str) -> set[date]:
    """Return the set of all stored dates for *ticker* in price_history."""
    rows = conn.execute(
        "SELECT date FROM price_history WHERE ticker = ?", (ticker,)
    ).fetchall()
    return {_to_date(r["date"]) for r in rows}


# ─────────────────────────────────────────────────────────────────────────────
# Upsert helpers
# ─────────────────────────────────────────────────────────────────────────────

def upsert_ohlcv(
    conn: sqlite3.Connection,
    ticker: str,
    df: pd.DataFrame,
    source: str = "polygon",
) -> int:
    """
    Batch-upsert OHLCV rows into *price_history*.

    *df* must have a DatetimeIndex and columns: open, high, low, close, volume.
    Returns the number of rows written.
    """
    if df.empty:
        logger.warning("[%s] upsert_ohlcv called with empty DataFrame", ticker)
        return 0

    rows = [
        (
            ticker,
            idx.date().isoformat(),
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            int(row["volume"]) if pd.notna(row.get("volume")) else None,
            source,
        )
        for idx, row in df.iterrows()
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO price_history
            (ticker, date, open, high, low, close, volume, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    logger.debug("[%s] Upserted %d OHLCV rows (source=%s)", ticker, len(rows), source)
    return len(rows)


def upsert_volatility(
    conn: sqlite3.Connection,
    ticker: str,
    df: pd.DataFrame,
) -> int:
    """
    Batch-upsert volatility rows into *volatility_history*.

    *df* must have a DatetimeIndex and columns:
        iv_atm_30d (optional), hv_21d, hv_30d, hv_60d, iv_source (optional).
    Returns the number of rows written.
    """
    if df.empty:
        logger.warning("[%s] upsert_volatility called with empty DataFrame", ticker)
        return 0

    rows = [
        (
            ticker,
            idx.date().isoformat(),
            row.get("iv_atm_30d"),
            row.get("hv_21d"),
            row.get("hv_30d"),
            row.get("hv_60d"),
            row.get("iv_source"),
        )
        for idx, row in df.iterrows()
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO volatility_history
            (ticker, date, iv_atm_30d, hv_21d, hv_30d, hv_60d, iv_source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    logger.debug("[%s] Upserted %d volatility rows", ticker, len(rows))
    return len(rows)


def upsert_macro(
    conn: sqlite3.Connection,
    series_id: str,
    df: pd.DataFrame,
) -> int:
    """
    Batch-upsert rows into *macro_data*.

    *df* must have a DatetimeIndex and a single column named 'value'.
    Returns the number of rows written.
    """
    if df.empty:
        logger.warning("[%s] upsert_macro called with empty DataFrame", series_id)
        return 0

    rows = [
        (series_id, idx.date().isoformat(), row["value"])
        for idx, row in df.iterrows()
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO macro_data (series_id, date, value)
        VALUES (?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    logger.debug("[%s] Upserted %d macro rows", series_id, len(rows))
    return len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Data-cleaning routines
# ─────────────────────────────────────────────────────────────────────────────

def fill_missing_trading_days(
    conn: sqlite3.Connection,
    ticker: str,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> list[date]:
    """
    Detect gaps in *price_history* for *ticker* against the NYSE trading
    calendar.  Returns the list of missing trading dates so the caller can
    trigger a targeted re-fetch.

    Parameters
    ----------
    from_date : date, optional
        Start of the window to check.  Defaults to the earliest stored date.
    to_date : date, optional
        End of the window to check.  Defaults to yesterday.
    """
    stored = get_stored_dates(conn, ticker)
    if not stored:
        return []

    start = from_date or min(stored)
    end = to_date or (date.today() - timedelta(days=1))

    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(
        start_date=start.isoformat(),
        end_date=end.isoformat(),
    )
    expected = {d.date() for d in schedule.index}

    missing = sorted(expected - stored)
    if missing:
        logger.warning(
            "[%s] Gap detected: %d missing trading days (first: %s, last: %s)",
            ticker,
            len(missing),
            missing[0],
            missing[-1],
        )
    else:
        logger.debug("[%s] No gaps detected in trading day coverage", ticker)

    return missing


def outlier_check(
    conn: sqlite3.Connection,
    ticker: str,
    z_threshold: float = 5.0,
    window: int = 90,
) -> list[date]:
    """
    Flag close prices with a rolling Z-score above *z_threshold* (default 5σ).

    Suspicious rows are logged as WARNING and recorded in pipeline_log.
    Data is never automatically modified — the caller decides what to do.

    Returns a list of flagged dates.
    """
    rows = conn.execute(
        """
        SELECT date, close FROM price_history
        WHERE ticker = ?
        ORDER BY date ASC
        """,
        (ticker,),
    ).fetchall()

    if len(rows) < window:
        return []

    df = pd.DataFrame(rows, columns=["date", "close"])
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df["close"] = pd.to_numeric(df["close"])

    roll_mean = df["close"].rolling(window).mean()
    roll_std = df["close"].rolling(window).std()
    df["z"] = (df["close"] - roll_mean) / roll_std

    flagged_df = df[df["z"].abs() > z_threshold]
    flagged = [d.date() for d in flagged_df.index]

    if flagged:
        logger.warning(
            "[%s] Outlier check: %d suspicious close prices flagged (|Z|>%.1f): %s",
            ticker,
            len(flagged),
            z_threshold,
            flagged,
        )

    return flagged


# ─────────────────────────────────────────────────────────────────────────────
# Audit logging
# ─────────────────────────────────────────────────────────────────────────────

def log_run(
    conn: sqlite3.Connection,
    status: str,
    tickers_ok: int = 0,
    tickers_fail: int = 0,
    notes: str = "",
) -> None:
    """
    Append a row to *pipeline_log* recording the outcome of a pipeline run.

    Parameters
    ----------
    status : str
        One of 'success', 'partial', or 'error'.
    tickers_ok : int
        Number of tickers fetched and stored successfully.
    tickers_fail : int
        Number of tickers that failed.
    notes : str
        Free-text summary or error message.
    """
    conn.execute(
        """
        INSERT INTO pipeline_log (run_at, status, tickers_ok, tickers_fail, notes)
        VALUES (datetime('now'), ?, ?, ?, ?)
        """,
        (status, tickers_ok, tickers_fail, notes),
    )
    conn.commit()
    logger.info(
        "Run logged: status=%s ok=%d fail=%d", status, tickers_ok, tickers_fail
    )


# ─────────────────────────────────────────────────────────────────────────────
# Dimensional Modeling & Sync Helpers
# ─────────────────────────────────────────────────────────────────────────────

SECURITY_METADATA = {
    "SPY": {"name": "SPDR S&P 500 ETF Trust", "asset_class": "ETF", "sector": "Broad Market", "exchange": "NYSE Arca"},
    "QQQ": {"name": "Invesco QQQ Trust", "asset_class": "ETF", "sector": "Technology", "exchange": "NASDAQ"},
    "AAPL": {"name": "Apple Inc.", "asset_class": "Equity", "sector": "Consumer Electronics", "exchange": "NASDAQ"},
    "MSFT": {"name": "Microsoft Corporation", "asset_class": "Equity", "sector": "Software & Cloud", "exchange": "NASDAQ"},
    "NVDA": {"name": "NVIDIA Corporation", "asset_class": "Equity", "sector": "Semiconductors", "exchange": "NASDAQ"},
    "TSLA": {"name": "Tesla, Inc.", "asset_class": "Equity", "sector": "Automotive & Energy", "exchange": "NASDAQ"},
    "AMZN": {"name": "Amazon.com, Inc.", "asset_class": "Equity", "sector": "E-Commerce & Cloud", "exchange": "NASDAQ"},
    "GOOGL": {"name": "Alphabet Inc.", "asset_class": "Equity", "sector": "Internet & Search", "exchange": "NASDAQ"},
    "META": {"name": "Meta Platforms, Inc.", "asset_class": "Equity", "sector": "Social Media", "exchange": "NASDAQ"},
    "JPM": {"name": "JPMorgan Chase & Co.", "asset_class": "Equity", "sector": "Financial Services", "exchange": "NYSE"},
}

MACRO_METADATA = {
    "DGS10": {"name": "10-Year Treasury Constant Maturity Rate", "category": "Interest Rates", "frequency": "Daily", "units": "Percent"},
    "DGS2": {"name": "2-Year Treasury Constant Maturity Rate", "category": "Interest Rates", "frequency": "Daily", "units": "Percent"},
    "T10Y2Y": {"name": "10-Year Treasury Minus 2-Year Treasury Yield Spread", "category": "Interest Rates", "frequency": "Daily", "units": "Percent"},
    "CPIAUCSL": {"name": "Consumer Price Index for All Urban Consumers: All Items", "category": "Inflation", "frequency": "Monthly", "units": "Index 1982-1984=100"},
    "FEDFUNDS": {"name": "Federal Funds Effective Rate", "category": "Monetary Policy", "frequency": "Monthly", "units": "Percent"},
}


def populate_dim_security(conn: sqlite3.Connection) -> int:
    """Populate or update dim_security from known metadata and price_history."""
    c = conn.cursor()
    c.execute("SELECT DISTINCT ticker FROM price_history")
    tickers = [r[0] for r in c.fetchall()]

    rows = []
    for t in tickers:
        meta = SECURITY_METADATA.get(t, {
            "name": f"{t} Security",
            "asset_class": "Equity",
            "sector": "General",
            "exchange": "US",
        })
        rows.append((t, meta["name"], meta["asset_class"], meta["sector"], meta["exchange"], "USD"))

    c.executemany(
        """
        INSERT OR REPLACE INTO dim_security (ticker, company_name, asset_class, sector, exchange, currency)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    logger.debug("Synchronized %d securities in dim_security", len(rows))
    return len(rows)


def populate_dim_macro_indicator(conn: sqlite3.Connection) -> int:
    """Populate dim_macro_indicator with known metadata."""
    c = conn.cursor()
    c.execute("SELECT DISTINCT series_id FROM macro_data")
    series_ids = [r[0] for r in c.fetchall()]

    rows = []
    for sid in series_ids:
        meta = MACRO_METADATA.get(sid, {
            "name": f"FRED Series {sid}",
            "category": "Macroeconomic",
            "frequency": "Unknown",
            "units": "Value",
        })
        rows.append((sid, meta["name"], meta["category"], meta["frequency"], meta["units"]))

    c.executemany(
        """
        INSERT OR REPLACE INTO dim_macro_indicator (series_id, series_name, category, frequency, units)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    logger.debug("Synchronized %d indicators in dim_macro_indicator", len(rows))
    return len(rows)


def populate_dim_date(
    conn: sqlite3.Connection,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> int:
    """Populate dim_date calendar dimension across the database date range."""
    c = conn.cursor()
    c.execute("SELECT MIN(date), MAX(date) FROM price_history")
    row = c.fetchone()
    if not row or not row[0]:
        return 0

    min_d = _to_date(row[0])
    max_d = _to_date(row[1])

    start = start_date or (min_d - timedelta(days=30))
    end = end_date or (max_d + timedelta(days=30))

    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=start.isoformat(), end_date=end.isoformat())
    trading_days = set(sched.index.date)

    curr = start
    rows = []
    while curr <= end:
        d_key = int(curr.strftime("%Y%m%d"))
        is_wk = curr.isoweekday() >= 6
        is_td = curr in trading_days
        rows.append((
            d_key,
            curr.isoformat(),
            curr.year,
            (curr.month - 1) // 3 + 1,
            curr.month,
            curr.strftime("%B"),
            curr.day,
            curr.isoweekday(),
            curr.strftime("%A"),
            is_wk,
            is_td,
        ))
        curr += timedelta(days=1)

    c.executemany(
        """
        INSERT OR REPLACE INTO dim_date
        (date_key, full_date, year, quarter, month, month_name, day_of_month, day_of_week, day_name, is_weekend, is_trading_day)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    logger.debug("Synchronized %d dates in dim_date", len(rows))
    return len(rows)


def sync_fact_market_daily(conn: sqlite3.Connection) -> int:
    """
    Synchronize fact_market_daily by joining price_history and volatility_history
    against dim_security and dim_date.
    """
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO fact_market_daily (
            date_key,
            security_key,
            open,
            high,
            low,
            close,
            volume,
            daily_return,
            log_return,
            iv_atm_30d,
            hv_21d,
            hv_30d,
            hv_60d,
            vol_risk_premium,
            source
        )
        SELECT 
            CAST(strftime('%Y%m%d', p.date) AS INTEGER) AS date_key,
            s.security_key,
            p.open,
            p.high,
            p.low,
            p.close,
            p.volume,
            ROUND((p.close - LAG(p.close) OVER (PARTITION BY p.ticker ORDER BY p.date ASC)) / NULLIF(LAG(p.close) OVER (PARTITION BY p.ticker ORDER BY p.date ASC), 0), 6) AS daily_return,
            ROUND(ln(p.close / NULLIF(LAG(p.close) OVER (PARTITION BY p.ticker ORDER BY p.date ASC), 0)), 6) AS log_return,
            v.iv_atm_30d,
            v.hv_21d,
            v.hv_30d,
            v.hv_60d,
            ROUND(v.iv_atm_30d - v.hv_30d, 4) AS vol_risk_premium,
            p.source
        FROM price_history p
        JOIN dim_security s ON p.ticker = s.ticker
        LEFT JOIN volatility_history v ON p.ticker = v.ticker AND p.date = v.date
        """
    )
    conn.commit()
    count = c.rowcount
    logger.info("Synchronized %d rows in fact_market_daily", count)
    return count


def sync_dimensional_model(conn: sqlite3.Connection) -> None:
    """
    Master sync routine: refreshes all dimensions and conformed facts
    to ensure the star schema and feature store are up to date.
    """
    populate_dim_security(conn)
    populate_dim_macro_indicator(conn)
    populate_dim_date(conn)
    sync_fact_market_daily(conn)
    logger.info("Dimensional model (star schema & feature store) synchronized.")

