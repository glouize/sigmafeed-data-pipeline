"""
data_fetcher.py — API Integration Module
==========================================
Fetches market and macroeconomic data from three sources:

  1. Polygon.io  — primary OHLCV and ATM IV (options snapshot)
  2. yfinance    — OHLCV fallback when Polygon fails or quota is exceeded
  3. FRED        — macroeconomic time series (Treasury yields, CPI, etc.)

All public functions return a clean pd.DataFrame with a DatetimeIndex and
standardised column names.  Split/dividend adjustments are applied at fetch
time so stored values are always fully adjusted.

Rate-limit handling
-------------------
  Polygon free tier : 5 requests / minute
    → exponential back-off + jitter (base^attempt + U[0,1] seconds)
    → up to `polygon_max_retries` attempts before raising PolygonError

  FRED             : 120 requests / minute (generous)
    → simple sleep of `fred_sleep_seconds` between series calls
"""

from __future__ import annotations

import logging
import math
import os
import random
import time
from datetime import date, timedelta
from typing import Any, Optional

import pandas as pd
import requests
import yfinance as yf

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────────────────────


class PolygonError(RuntimeError):
    """Raised when all Polygon retry attempts are exhausted."""


class FredError(RuntimeError):
    """Raised when a FRED API call fails."""


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────


def _get_polygon_key() -> str:
    key = os.environ.get("POLYGON_API_KEY", "")
    if not key:
        raise EnvironmentError(
            "POLYGON_API_KEY is not set. "
            "Copy .env.example to .env and add your key."
        )
    return key


def _get_fred_key() -> str:
    key = os.environ.get("FRED_API_KEY", "")
    if not key:
        raise EnvironmentError(
            "FRED_API_KEY is not set. "
            "Copy .env.example to .env and add your key."
        )
    return key


def _polygon_get(
    url: str,
    params: dict[str, Any],
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> dict:
    """
    Make a GET request to Polygon with exponential back-off retry.

    Raises PolygonError after *max_retries* failed attempts.
    """
    params["apiKey"] = _get_polygon_key()

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 403:
                # Access denied — retrying won't help (plan restriction).
                raise PolygonError(
                    f"Polygon 403 Forbidden: your plan does not have access to {url}. "
                    "Upgrade at https://polygon.io/dashboard/subscriptions"
                )

            if resp.status_code == 429:
                # Rate-limited: back off and retry
                wait = backoff_base ** attempt + random.random()
                logger.warning(
                    "Polygon rate-limited (429). Attempt %d/%d. Sleeping %.1fs.",
                    attempt,
                    max_retries,
                    wait,
                )
                time.sleep(wait)
                continue

            # Other HTTP errors — log and retry
            logger.warning(
                "Polygon HTTP %d on attempt %d/%d: %s",
                resp.status_code,
                attempt,
                max_retries,
                url,
            )
            time.sleep(backoff_base ** attempt + random.random())

        except requests.RequestException as exc:
            wait = backoff_base ** attempt + random.random()
            logger.warning(
                "Polygon request error on attempt %d/%d: %s. Retrying in %.1fs.",
                attempt,
                max_retries,
                exc,
                wait,
            )
            time.sleep(wait)

    raise PolygonError(
        f"Polygon request failed after {max_retries} attempts: {url}"
    )


def _to_date_str(d: date) -> str:
    return d.isoformat()


def _standardise_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure consistent column names and DatetimeIndex."""
    df.index = pd.to_datetime(df.index)
    df.index.name = "date"
    return df[["open", "high", "low", "close", "volume"]].copy()


# ─────────────────────────────────────────────────────────────────────────────
# OHLCV — Polygon
# ─────────────────────────────────────────────────────────────────────────────


def fetch_ohlcv_polygon(
    ticker: str,
    from_date: date,
    to_date: date,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> pd.DataFrame:
    """
    Fetch daily OHLCV bars from Polygon.io for *ticker* over [from_date, to_date].

    Uses adjusted prices (splits and dividends applied).
    Returns a DataFrame with DatetimeIndex and columns: open, high, low, close, volume.

    Raises PolygonError on failure.
    """
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day"
        f"/{_to_date_str(from_date)}/{_to_date_str(to_date)}"
    )
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
    }

    data = _polygon_get(url, params, max_retries=max_retries, backoff_base=backoff_base)

    results = data.get("results", [])
    if not results:
        raise PolygonError(f"No results returned for {ticker} ({from_date} – {to_date})")

    df = pd.DataFrame(results)
    # Polygon field names: o, h, l, c, v, t (epoch ms)
    df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
    df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_localize(None)
    df.set_index("date", inplace=True)
    df = df[["open", "high", "low", "close", "volume"]]

    logger.info(
        "[%s] Polygon OHLCV: %d rows (%s – %s)",
        ticker,
        len(df),
        from_date,
        to_date,
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# OHLCV — yfinance (fallback)
# ─────────────────────────────────────────────────────────────────────────────


def fetch_ohlcv_yfinance(
    ticker: str,
    from_date: date,
    to_date: date,
) -> pd.DataFrame:
    """
    Fetch daily OHLCV bars via yfinance for *ticker* over [from_date, to_date].

    auto_adjust=True applies splits and dividends.
    Returns a DataFrame with DatetimeIndex and columns: open, high, low, close, volume.
    """
    raw = yf.download(
        ticker,
        start=_to_date_str(from_date),
        # yfinance end date is exclusive, so add one day
        end=_to_date_str(to_date + timedelta(days=1)),
        auto_adjust=True,
        progress=False,
        threads=False,
    )

    if raw.empty:
        raise ValueError(f"yfinance returned no data for {ticker}")

    # yfinance column names depend on version; normalise to lowercase
    raw.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in raw.columns]
    raw.index = pd.to_datetime(raw.index)
    raw.index.name = "date"
    df = raw[["open", "high", "low", "close", "volume"]].copy()

    logger.info(
        "[%s] yfinance OHLCV: %d rows (%s – %s)",
        ticker,
        len(df),
        from_date,
        to_date,
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# OHLCV — dispatcher (Polygon → yfinance fallback)
# ─────────────────────────────────────────────────────────────────────────────


def fetch_ohlcv(
    ticker: str,
    from_date: date,
    to_date: date,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch daily OHLCV data, trying Polygon first with silent fallback to yfinance.

    Returns
    -------
    (df, source)
        df     : DataFrame with DatetimeIndex, columns open/high/low/close/volume
        source : 'polygon' or 'yfinance'
    """
    try:
        df = fetch_ohlcv_polygon(ticker, from_date, to_date, max_retries, backoff_base)
        return df, "polygon"
    except (PolygonError, Exception) as exc:
        logger.warning(
            "[%s] Polygon OHLCV failed (%s). Falling back to yfinance.", ticker, exc
        )

    df = fetch_ohlcv_yfinance(ticker, from_date, to_date)
    return df, "yfinance"


# ─────────────────────────────────────────────────────────────────────────────
# Implied Volatility — ATM 30-day (Polygon options snapshot)
# ─────────────────────────────────────────────────────────────────────────────


def fetch_iv_atm(
    ticker: str,
    as_of_date: date,
    spot_price: float,
    atm_tolerance_pct: float = 0.02,
    expiry_days_min: int = 25,
    expiry_days_max: int = 35,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> Optional[float]:
    """
    Fetch the ATM 30-day implied volatility for *ticker* from Polygon's
    options snapshot endpoint.

    ATM selection criteria
    ----------------------
    - Expiry between *expiry_days_min* and *expiry_days_max* trading days out
    - Strike within *atm_tolerance_pct* of *spot_price* (default ±2%)
    - Both calls and puts averaged (put-call symmetry)

    Returns the annualised IV as a decimal (e.g. 0.25 = 25%), or None if no
    qualifying contracts are found.
    """
    url = f"https://api.polygon.io/v3/snapshot/options/{ticker}"

    # Expiry window around ~30 calendar days from the as_of date
    exp_min = as_of_date + timedelta(days=expiry_days_min)
    exp_max = as_of_date + timedelta(days=expiry_days_max)
    strike_lo = spot_price * (1 - atm_tolerance_pct)
    strike_hi = spot_price * (1 + atm_tolerance_pct)

    params = {
        "expiration_date.gte": _to_date_str(exp_min),
        "expiration_date.lte": _to_date_str(exp_max),
        "strike_price.gte": round(strike_lo, 2),
        "strike_price.lte": round(strike_hi, 2),
        "limit": 250,
    }

    try:
        data = _polygon_get(url, params, max_retries=max_retries, backoff_base=backoff_base)
    except PolygonError as exc:
        logger.warning("[%s] IV fetch failed: %s. Storing NULL.", ticker, exc)
        return None

    results = data.get("results", [])
    if not results:
        logger.warning(
            "[%s] No ATM options found for expiry %s–%s, strike %.2f–%.2f. Storing NULL.",
            ticker,
            exp_min,
            exp_max,
            strike_lo,
            strike_hi,
        )
        return None

    ivs = []
    for r in results:
        day = r.get("day") or {}
        greeks = r.get("greeks") or {}
        iv = r.get("implied_volatility") or greeks.get("implied_volatility")
        if iv and iv > 0:
            ivs.append(float(iv))

    if not ivs:
        logger.warning(
            "[%s] Options snapshot returned results but no valid IV values. Storing NULL.",
            ticker,
        )
        return None

    atm_iv = sum(ivs) / len(ivs)
    logger.info(
        "[%s] ATM IV (30d): %.4f from %d contracts", ticker, atm_iv, len(ivs)
    )
    return atm_iv


# ─────────────────────────────────────────────────────────────────────────────
# Realized / Historical Volatility
# ─────────────────────────────────────────────────────────────────────────────


def compute_hv(
    ohlcv_df: pd.DataFrame,
    windows: list[int] | None = None,
) -> pd.DataFrame:
    """
    Compute trailing close-to-close Realized / Historical Volatility (HV)
    for one or more rolling windows.

    Formula
    -------
    HV(n) = std( ln(close_t / close_{t-1}), window=n ) × sqrt(252)

    Parameters
    ----------
    ohlcv_df : pd.DataFrame
        Must have a DatetimeIndex and a 'close' column (adjusted prices).
    windows : list[int]
        Rolling windows in trading days.  Default: [21, 30, 60].

    Returns
    -------
    pd.DataFrame
        Columns: hv_21d, hv_30d, hv_60d (or matching *windows* names).
        Index matches *ohlcv_df*.
    """
    if windows is None:
        windows = [21, 30, 60]

    log_ret = ohlcv_df["close"].apply(math.log).diff()
    result = pd.DataFrame(index=ohlcv_df.index)

    for w in windows:
        col = f"hv_{w}d"
        result[col] = log_ret.rolling(w).std() * math.sqrt(252)

    logger.debug("Computed HV for windows %s (%d rows)", windows, len(result))
    return result


# ─────────────────────────────────────────────────────────────────────────────
# FRED — Macroeconomic data
# ─────────────────────────────────────────────────────────────────────────────


def fetch_fred_series(
    series_id: str,
    from_date: date,
    to_date: date,
    sleep_seconds: float = 0.5,
) -> pd.DataFrame:
    """
    Fetch a FRED time series via the fredapi library.

    Weekend / holiday gaps are forward-filled (FRED daily series like DGS10
    are not published on non-business days).

    Returns a DataFrame with DatetimeIndex and a single 'value' column.
    """
    try:
        import fredapi  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "fredapi is required: pip install fredapi"
        ) from exc

    fred = fredapi.Fred(api_key=_get_fred_key())

    time.sleep(sleep_seconds)  # polite pacing

    raw = fred.get_series(
        series_id,
        observation_start=_to_date_str(from_date),
        observation_end=_to_date_str(to_date),
    )

    if raw is None or raw.empty:
        logger.warning("[%s] FRED returned no data for %s – %s", series_id, from_date, to_date)
        return pd.DataFrame(columns=["value"])

    df = raw.rename("value").to_frame()
    df.index = pd.to_datetime(df.index)
    df.index.name = "date"

    # Forward-fill weekends and federal holidays
    full_idx = pd.date_range(start=df.index.min(), end=df.index.max(), freq="D")
    df = df.reindex(full_idx).ffill()
    df.index.name = "date"

    # Drop rows before from_date or after to_date (reindex can expand)
    df = df.loc[
        (df.index >= pd.Timestamp(from_date))
        & (df.index <= pd.Timestamp(to_date))
    ]

    logger.info(
        "[%s] FRED: %d rows (%s – %s)", series_id, len(df), from_date, to_date
    )
    return df


def fetch_all_macro(
    series_ids: list[str],
    from_date: date,
    to_date: date,
    sleep_seconds: float = 0.5,
) -> dict[str, pd.DataFrame]:
    """
    Fetch multiple FRED series and return a dict keyed by series_id.

    Failed series are logged and skipped (not raised) so one bad series
    does not abort the whole macro pull.
    """
    results: dict[str, pd.DataFrame] = {}

    for sid in series_ids:
        try:
            df = fetch_fred_series(sid, from_date, to_date, sleep_seconds)
            results[sid] = df
        except Exception as exc:
            logger.error("[%s] FRED fetch failed: %s", sid, exc)

    return results
