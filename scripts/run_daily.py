"""
run_daily.py - Pipeline Orchestrator
======================================
Entry point for the market data pipeline.  Fetches, cleans, and stores
daily OHLCV, volatility, and macroeconomic data into the SQLite database.

Usage
-----
    python run_daily.py                          # incremental update
    python run_daily.py --full-reload            # re-fetch all history
    python run_daily.py --tickers AAPL MSFT SPY  # override ticker list
    python run_daily.py --dry-run                # fetch only, no DB writes
    python run_daily.py --verbose                # debug-level logging
    python run_daily.py --export-csv             # export updated data to CSV

Exit codes
----------
    0  All tickers and macro fetched successfully
    1  Partial success (>=1 ticker failed; macro OK or vice-versa)
    2  Fatal error (missing config, DB error, etc.)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Add src to pythonpath so sigmafeed is importable
repo_root = Path(__file__).resolve().parent
if repo_root.name == "scripts":
    repo_root = repo_root.parent
sys.path.insert(0, str(repo_root / "src"))

try:
    from sigmafeed.fetchers import data_fetcher as fetcher
    from sigmafeed.storage import database as db
    from sigmafeed.exporters import exporter
except ImportError:
    import data_fetcher as fetcher
    import database as db
    exporter = None

# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────

_LOG_FORMAT = "[%(asctime)s] [%(levelname)-8s] %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _setup_logging(log_dir: str, verbose: bool, retention_days: int = 30) -> None:
    """Configure root logger: console + daily-rotating file handler."""
    level = logging.DEBUG if verbose else logging.INFO
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(logging.Formatter(_LOG_FORMAT, _DATE_FORMAT))
    root.addHandler(console)

    # File handler (daily rotation, keep N days)
    file_handler = TimedRotatingFileHandler(
        filename=str(log_path / "pipeline.log"),
        when="midnight",
        backupCount=retention_days,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(_LOG_FORMAT, _DATE_FORMAT))
    root.addHandler(file_handler)


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Config loader
# ─────────────────────────────────────────────────────────────────────────────


def _load_config(config_path: str = "config/config.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        path = Path("config.yaml")
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found at '{config_path}' or 'config.yaml'. "
            "Ensure config/config.yaml is present."
        )
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# Fetch window helpers
# ─────────────────────────────────────────────────────────────────────────────


def _resolve_fetch_window(
    conn,
    ticker: str,
    full_reload: bool,
    default_history_days: int,
) -> tuple[date, date]:
    """
    Determine the [from_date, to_date] window for a given ticker.

    - full_reload=True  → from (today - default_history_days) to today
    - Otherwise         → from (last_stored_date + 1) to today
                          or (today - default_history_days) if no history
    """
    today = date.today()
    to_date = today

    if full_reload:
        from_date = today - timedelta(days=default_history_days)
    else:
        last = db.get_last_date(conn, ticker)
        if last is None:
            from_date = today - timedelta(days=default_history_days)
        else:
            from_date = last + timedelta(days=1)

    return from_date, to_date


# ─────────────────────────────────────────────────────────────────────────────
# Per-ticker pipeline
# ─────────────────────────────────────────────────────────────────────────────


def _process_ticker(
    conn,
    ticker: str,
    from_date: date,
    to_date: date,
    cfg: dict,
    dry_run: bool,
) -> bool:
    """
    Fetch OHLCV + IV + HV for a single ticker and store results.

    Returns True on success, False on failure.
    """
    if from_date > to_date:
        logger.info("[%s] Already up to date. Skipping.", ticker)
        return True

    logger.info("[%s] Fetching %s -> %s", ticker, from_date, to_date)

    # 1. OHLCV
    try:
        ohlcv_df, source = fetcher.fetch_ohlcv(
            ticker,
            from_date,
            to_date,
            max_retries=cfg["fetch"]["polygon_max_retries"],
            backoff_base=cfg["fetch"]["polygon_backoff_base"],
        )
    except Exception as exc:
        logger.error("[%s] OHLCV fetch completely failed: %s", ticker, exc)
        return False

    if ohlcv_df.empty:
        logger.warning("[%s] OHLCV returned empty — skipping volatility.", ticker)
        return False

    # 2. ATM IV (from Polygon options snapshot for the latest date)
    iv_value: float | None = None
    iv_source: str | None = None

    latest_close = float(ohlcv_df["close"].iloc[-1])
    latest_date = ohlcv_df.index[-1].date()

    if source == "polygon":
        try:
            iv_value = fetcher.fetch_iv_atm(
                ticker,
                as_of_date=latest_date,
                spot_price=latest_close,
                atm_tolerance_pct=cfg.get("iv_atm_tolerance_pct", 0.02),
                expiry_days_min=cfg.get("iv_expiry_days_min", 25),
                expiry_days_max=cfg.get("iv_expiry_days_max", 35),
                max_retries=cfg["fetch"]["polygon_max_retries"],
                backoff_base=cfg["fetch"]["polygon_backoff_base"],
            )
            if iv_value is not None:
                iv_source = "polygon_options"
        except Exception as exc:
            logger.warning("[%s] IV fetch error: %s. Storing NULL.", ticker, exc)
    else:
        logger.info("[%s] IV skipped (source=yfinance, no options data).", ticker)

    # 3. Historical Volatility (local computation)
    hv_windows = cfg.get("hv_windows", [21, 30, 60])

    # To get accurate rolling HV we need all historical closes, not just the
    # new window.  Pull the stored history and append the fresh data.
    try:
        stored_rows = conn.execute(
            "SELECT date, close FROM price_history WHERE ticker = ? ORDER BY date ASC",
            (ticker,),
        ).fetchall()
        if stored_rows:
            import pandas as pd  # noqa: PLC0415

            stored_df = pd.DataFrame(stored_rows, columns=["date", "close"])
            stored_df["date"] = pd.to_datetime(stored_df["date"])
            stored_df.set_index("date", inplace=True)
            combined = stored_df.join(
                ohlcv_df[["close"]].rename(columns={"close": "close_new"}),
                how="outer",
            )
            combined["close"] = combined["close"].fillna(combined["close_new"])
            combined = combined[["close"]]
        else:
            combined = ohlcv_df[["close"]].copy()

        hv_df = fetcher.compute_hv(combined, windows=hv_windows)
        # Only keep rows that correspond to the new data being upserted
        hv_df = hv_df.loc[hv_df.index.isin(ohlcv_df.index)]
    except Exception as exc:
        logger.warning("[%s] HV computation failed: %s", ticker, exc)
        hv_df = ohlcv_df[[]].copy()  # empty frame with same index

    # 4. Build volatility DataFrame to store
    import pandas as pd  # noqa: PLC0415

    vol_df = hv_df.copy()
    vol_df["iv_atm_30d"] = None
    vol_df["iv_source"] = None

    if iv_value is not None:
        # Store IV only on the latest date row
        latest_ts = ohlcv_df.index[-1]
        if latest_ts in vol_df.index:
            vol_df.at[latest_ts, "iv_atm_30d"] = iv_value
            vol_df.at[latest_ts, "iv_source"] = iv_source

    # 5. Write to DB (unless dry-run)
    if not dry_run:
        db.upsert_ohlcv(conn, ticker, ohlcv_df, source=source)
        db.upsert_volatility(conn, ticker, vol_df)
    else:
        logger.info(
            "[%s] DRY-RUN: would write %d OHLCV rows + %d vol rows",
            ticker,
            len(ohlcv_df),
            len(vol_df),
        )

    # 6. Gap detection (only on existing data, not dry-run)
    if not dry_run:
        missing = db.fill_missing_trading_days(conn, ticker)
        if missing:
            logger.warning(
                "[%s] %d gap(s) detected — re-fetching missed dates.", ticker, len(missing)
            )
            _backfill_gaps(conn, ticker, missing, cfg, dry_run)

        # Outlier check
        db.outlier_check(conn, ticker)

    return True


def _backfill_gaps(
    conn,
    ticker: str,
    missing_dates: list[date],
    cfg: dict,
    dry_run: bool,
) -> None:
    """Fetch and store data for a list of specific missing trading dates."""
    if not missing_dates:
        return
    from_date = missing_dates[0]
    to_date = missing_dates[-1]
    logger.info("[%s] Back-filling %s → %s", ticker, from_date, to_date)
    try:
        ohlcv_df, source = fetcher.fetch_ohlcv(
            ticker, from_date, to_date,
            max_retries=cfg["fetch"]["polygon_max_retries"],
            backoff_base=cfg["fetch"]["polygon_backoff_base"],
        )
        if not dry_run and not ohlcv_df.empty:
            db.upsert_ohlcv(conn, ticker, ohlcv_df, source=source)
    except Exception as exc:
        logger.error("[%s] Back-fill failed: %s", ticker, exc)


# ─────────────────────────────────────────────────────────────────────────────
# Macro pipeline
# ─────────────────────────────────────────────────────────────────────────────


def _process_macro(
    conn,
    series_ids: list[str],
    from_date: date,
    to_date: date,
    cfg: dict,
    dry_run: bool,
) -> bool:
    """Fetch all FRED series and store them. Returns True on full success."""
    logger.info("Fetching FRED macro series: %s", series_ids)

    macro_data = fetcher.fetch_all_macro(
        series_ids,
        from_date,
        to_date,
        sleep_seconds=cfg["fetch"]["fred_sleep_seconds"],
    )

    if not macro_data:
        logger.error("All FRED series failed.")
        return False

    for sid, df in macro_data.items():
        if not dry_run:
            db.upsert_macro(conn, sid, df)
        else:
            logger.info("[%s] DRY-RUN: would write %d macro rows", sid, len(df))

    failed_series = set(series_ids) - set(macro_data.keys())
    if failed_series:
        logger.warning("FRED series that failed: %s", failed_series)
        return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Market Data Pipeline — daily runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--full-reload",
        action="store_true",
        help="Re-fetch full history for all tickers (ignores last stored date).",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        metavar="TICKER",
        help="Override the ticker list from config.yaml.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not write anything to the database.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config file (default: config/config.yaml).",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export all successfully updated tickers to CSV in exports/ folder.",
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────


def main() -> int:
    args = _parse_args()

    # Load .env (no error if file is missing — env vars may be set externally)
    load_dotenv(override=False)

    # Load config
    try:
        cfg = _load_config(args.config)
    except FileNotFoundError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2

    # Setup logging
    log_cfg = cfg.get("logging", {})
    _setup_logging(
        log_dir=log_cfg.get("log_dir", "logs"),
        verbose=args.verbose,
        retention_days=log_cfg.get("retention_days", 30),
    )

    if args.dry_run:
        logger.info("=== DRY-RUN MODE — no data will be written ===")

    # Resolve ticker list
    tickers: list[str] = args.tickers or cfg.get("tickers", [])
    if not tickers:
        logger.error("No tickers configured. Add tickers to config.yaml.")
        return 2

    fred_series: list[str] = cfg.get("fred_series", [])
    db_path: str = cfg["database"]["path"]
    default_history_days: int = cfg["fetch"].get("default_history_days", 365)

    # Initialise database
    try:
        conn = db.init_db(db_path)
    except Exception as exc:
        logger.critical("Failed to initialise database: %s", exc)
        return 2

    try:
        today = date.today()
        macro_from = today - timedelta(days=default_history_days)

        # ── Ticker loop ───────────────────────────────────────────────────────────
        ok_tickers: list[str] = []
        fail_tickers: list[str] = []

        for ticker in tickers:
            from_date, to_date = _resolve_fetch_window(
                conn, ticker, args.full_reload, default_history_days
            )
            success = _process_ticker(conn, ticker, from_date, to_date, cfg, args.dry_run)
            if success:
                ok_tickers.append(ticker)
            else:
                fail_tickers.append(ticker)

        # ── Macro ─────────────────────────────────────────────────────────────────
        macro_ok = True
        if fred_series:
            macro_ok = _process_macro(
                conn, fred_series, macro_from, today, cfg, args.dry_run
            )
        else:
            logger.info("No FRED series configured — skipping macro fetch.")

        # ── Dimensional Model Sync ────────────────────────────────────────────────
        if not args.dry_run and ok_tickers:
            try:
                logger.info("Synchronizing dimensional model (star schema & feature store)...")
                db.sync_dimensional_model(conn)
            except Exception as exc:
                logger.warning("Dimensional sync encountered warning: %s", exc)

        # ── Summary ───────────────────────────────────────────────────────────────
        logger.info("-" * 60)
        logger.info(
            "Run complete: %d/%d tickers OK | macro: %s",
            len(ok_tickers),
            len(tickers),
            "OK" if macro_ok else "PARTIAL/FAILED",
        )
        if fail_tickers:
            logger.warning("Failed tickers: %s", fail_tickers)
        logger.info("-" * 60)

        # Export to CSV if requested
        if args.export_csv and exporter and ok_tickers:
            logger.info("Exporting updated ticker data to CSV in exports/ ...")
            for t in ok_tickers:
                try:
                    out = exporter.export_ticker_to_csv(db_path, t)
                    logger.info("[%s] Exported to %s", t, out)
                except Exception as exc:
                    logger.warning("[%s] Failed to export CSV: %s", t, exc)

        # Determine exit code and status
        if fail_tickers or not macro_ok:
            status = "partial"
            if not args.dry_run:
                db.log_run(
                    conn,
                    status=status,
                    tickers_ok=len(ok_tickers),
                    tickers_fail=len(fail_tickers),
                    notes=f"Failed: {fail_tickers}; macro_ok={macro_ok}",
                )
            return 1

        status = "success"
        if not args.dry_run:
            db.log_run(
                conn,
                status=status,
                tickers_ok=len(ok_tickers),
                tickers_fail=0,
                notes="",
            )
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
