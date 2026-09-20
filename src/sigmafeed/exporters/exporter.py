"""
exporter.py — Data Export Utilities
=====================================
Exports stored market data and analytics from SQLite to CSV files.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


def export_ticker_to_csv(
    db_path: str,
    ticker: str,
    output_path: Optional[str] = None,
) -> Path:
    """
    Export joined price, volatility, and benchmark macro yield data
    for a specific ticker to a CSV file.
    """
    if output_path is None:
        out_file = Path("exports") / f"{ticker}_market_data.csv"
    else:
        out_file = Path(output_path)

    out_file.parent.mkdir(parents=True, exist_ok=True)

    query = """
    SELECT 
        p.date,
        p.ticker,
        p.open,
        p.high,
        p.low,
        p.close,
        p.volume,
        p.source,
        v.iv_atm_30d,
        v.hv_21d,
        v.hv_30d,
        v.hv_60d,
        m.value AS treasury_10y_yield
    FROM price_history p
    LEFT JOIN volatility_history v 
        ON p.ticker = v.ticker AND p.date = v.date
    LEFT JOIN macro_data m 
        ON p.date = m.date AND m.series_id = 'DGS10'
    WHERE p.ticker = ?
    ORDER BY p.date DESC;
    """

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(query, conn, params=(ticker,))

    if df.empty:
        raise ValueError(f"No records found for ticker '{ticker}' in {db_path}")

    df.to_csv(out_file, index=False)
    logger.info("[%s] Exported %d rows to %s", ticker, len(df), out_file)
    return out_file


def export_table_to_csv(
    db_path: str,
    table_name: str,
    output_path: Optional[str] = None,
) -> Path:
    """
    Export an entire table from the SQLite database to CSV.
    """
    valid_tables = {"price_history", "volatility_history", "macro_data", "pipeline_log"}
    if table_name not in valid_tables:
        raise ValueError(f"Invalid table '{table_name}'. Valid options: {valid_tables}")

    if output_path is None:
        out_file = Path("exports") / f"{table_name}.csv"
    else:
        out_file = Path(output_path)

    out_file.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    df.to_csv(out_file, index=False)
    logger.info("Table '%s' (%d rows) exported to %s", table_name, len(df), out_file)
    return out_file
