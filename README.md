# Market Data Pipeline — User Guide

> **What this does:** Fetches daily OHLCV price data, Historical Volatility (HV), and macroeconomic indicators from Polygon.io, Yahoo Finance, and the Federal Reserve FRED API — and stores everything in a local SQLite database. Designed to run with a single command each day.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Installation](#2-installation)
3. [API Keys](#3-api-keys)
4. [Configuration](#4-configuration)
5. [Running the Pipeline](#5-running-the-pipeline)
6. [Understanding the Output](#6-understanding-the-output)
7. [Database Reference](#7-database-reference)
8. [Troubleshooting](#8-troubleshooting)
9. [Upgrade Path — Paid Features](#9-upgrade-path--paid-features)
10. [Daily Workflow](#10-daily-workflow)

---

## 1. Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.10 + | Check: `python --version` |
| pip | any | Bundled with Python |
| Internet access | — | For API calls |
| Polygon.io account | Free | [Sign up](https://polygon.io) |
| FRED account | Free | [Sign up](https://fred.stlouisfed.org/docs/api/api_key.html) |

> **No database server needed** — the pipeline uses SQLite, which is built into Python.

---

## 2. Installation

### Step 1 — Copy the project folder

```
amazing-raman/
├── .env.example      ← template for your API keys
├── config.yaml       ← tickers and settings
├── requirements.txt  ← Python dependencies
├── data_fetcher.py   ← API integration module
├── database.py       ← SQLite storage layer
└── run_daily.py      ← main runner script
```

### Step 2 — Create a virtual environment (recommended)

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python -m venv venv
source venv/bin/activate
```

### Step 3 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Set up your secrets file

```bash
# Windows
copy .env.example .env

# macOS / Linux
cp .env.example .env
```

Then open `.env` and fill in your two API keys (see Section 3).

---

## 3. API Keys

### Polygon.io (OHLCV data) — Free

1. Go to [https://polygon.io](https://polygon.io) → **Sign Up**
2. Navigate to **Dashboard → API Keys**
3. Copy your key and paste it into `.env`:

```
POLYGON_API_KEY=your_key_here
```

**Free plan includes:** Daily OHLCV for all US stocks/ETFs, 5 requests/minute.
**Not included on free plan:** Options data (needed for Implied Volatility — see Section 9).

---

### FRED API (macroeconomic data) — Free

1. Go to [https://fred.stlouisfed.org/docs/api/api_key.html](https://fred.stlouisfed.org/docs/api/api_key.html)
2. Click **Request API Key** — requires a free account
3. Your key arrives by email (usually instant)
4. Paste it into `.env`:

```
FRED_API_KEY=your_32_character_key_here
```

Your completed `.env` should look like:

```
POLYGON_API_KEY=abc123...
FRED_API_KEY=def456...
```

> **Never commit `.env` to git.** It is already in `.gitignore`.

---

## 4. Configuration

All settings live in `config.yaml`. Edit this file freely — no code changes are needed.

```yaml
database:
  path: data/market.db        # where the SQLite file is stored

tickers:                      # add or remove tickers here
  - SPY
  - QQQ
  - AAPL
  - MSFT
  - NVDA
  - TSLA
  - AMZN
  - GOOGL
  - META
  - JPM

fred_series:                  # FRED series IDs to fetch
  - DGS10      # 10-Year Treasury Yield (daily)
  - DGS2       # 2-Year Treasury Yield (daily)
  - T10Y2Y     # 10Y-2Y Spread (daily, recession signal)
  - CPIAUCSL   # CPI Inflation (monthly)
  - FEDFUNDS   # Fed Funds Rate (monthly)

iv_mode: atm_30d
hv_windows: [21, 30, 60]      # realized vol rolling windows (trading days)

fetch:
  default_history_days: 365   # how far back to fetch on first run
```

### Adding tickers

Just add lines to the `tickers:` list — no code changes needed:

```yaml
tickers:
  - SPY
  - AAPL
  - AMD      # new
  - NFLX     # new
```

Then run `--full-reload` once for the new tickers:
```bash
python run_daily.py --full-reload --tickers AMD NFLX
```

### Adding FRED series

Find any series ID at [https://fred.stlouisfed.org](https://fred.stlouisfed.org) and add it:

```yaml
fred_series:
  - DGS10
  - UNRATE    # Unemployment Rate
  - M2SL      # M2 Money Supply
```

---

## 5. Running the Pipeline

### First run — load full history

```bash
python run_daily.py --full-reload
```

Fetches 365 days of history for all tickers and FRED series.
**Expected runtime:** 30–60 seconds.

---

### Daily update — fetch only new data

```bash
python run_daily.py
```

Detects the last stored date per ticker and fetches only the delta.
**Expected runtime:** 5–15 seconds.

---

### All CLI commands

```bash
# Test everything without writing to the database
python run_daily.py --dry-run

# Fetch specific tickers only
python run_daily.py --tickers AAPL MSFT SPY

# Reload just specific tickers from scratch
python run_daily.py --full-reload --tickers NVDA AMD

# See detailed debug output
python run_daily.py --verbose

# Combine flags
python run_daily.py --dry-run --tickers SPY --verbose
```

---

## 6. Understanding the Output

### A successful run

```
[2026-09-20 18:00:01] [INFO] Database initialised at data/market.db
[2026-09-20 18:00:01] [INFO] [SPY] Fetching 2026-09-19 -> 2026-09-20
[2026-09-20 18:00:02] [INFO] [SPY] Polygon OHLCV: 1 rows
[2026-09-20 18:00:02] [WARN] [SPY] IV fetch failed: Polygon 403 Forbidden. Storing NULL.
...
[2026-09-20 18:00:15] [INFO] [DGS10] FRED: 1 rows
...
[2026-09-20 18:00:20] [INFO] Run complete: 10/10 tickers OK | macro: OK
[2026-09-20 18:00:20] [INFO] Run logged: status=success ok=10 fail=0
```

### Log levels

| Level | Meaning |
|---|---|
| `INFO` | Normal operation |
| `WARNING` | Non-fatal issue — fallback triggered or data unavailable |
| `ERROR` | A series or ticker failed (partial run) |
| `CRITICAL` | Fatal — DB or config missing |

### Exit codes

| Code | Meaning | Action |
|---|---|---|
| `0` | Full success | Nothing needed |
| `1` | Partial (1+ ticker or FRED series failed) | Check logs |
| `2` | Fatal error | Fix config/DB before next run |

### Common warnings (not errors)

**IV stored as NULL** — expected on the free Polygon plan:
```
[WARN] [SPY] IV fetch failed: Polygon 403 Forbidden. Storing NULL.
```

**yfinance fallback** — triggers when Polygon hits its 5 req/min limit:
```
[WARN] [TSLA] Polygon OHLCV failed. Falling back to yfinance.
[INFO] [TSLA] yfinance OHLCV: 250 rows
```
The row is tagged `source='yfinance'` in the DB for traceability.

---

## 7. Database Reference

The database is at `data/market.db`.
Open it with [DB Browser for SQLite](https://sqlitebrowser.org/) (free GUI tool).

### Tables

#### `price_history` — Daily OHLCV

| Column | Type | Description |
|---|---|---|
| `ticker` | TEXT | e.g. `SPY` |
| `date` | DATE | Trading date |
| `open` | REAL | Adjusted open price |
| `high` | REAL | Adjusted high |
| `low` | REAL | Adjusted low |
| `close` | REAL | Adjusted close price |
| `volume` | INTEGER | Daily volume |
| `source` | TEXT | `polygon` or `yfinance` |

#### `volatility_history` — IV and Realized Volatility

| Column | Type | Description |
|---|---|---|
| `ticker` | TEXT | e.g. `AAPL` |
| `date` | DATE | Trading date |
| `iv_atm_30d` | REAL | ATM 30-day implied vol (NULL on free plan) |
| `hv_21d` | REAL | 21-day realized vol (annualised %) |
| `hv_30d` | REAL | 30-day realized vol |
| `hv_60d` | REAL | 60-day realized vol |
| `iv_source` | TEXT | `polygon_options` or NULL |

#### `macro_data` — FRED Economic Indicators

| Column | Type | Description |
|---|---|---|
| `series_id` | TEXT | e.g. `DGS10` |
| `date` | DATE | Observation date |
| `value` | REAL | Indicator value |

#### `pipeline_log` — Run Audit Trail

| Column | Type | Description |
|---|---|---|
| `run_at` | DATETIME | When the run started |
| `status` | TEXT | `success`, `partial`, or `error` |
| `tickers_ok` | INTEGER | Tickers fetched successfully |
| `tickers_fail` | INTEGER | Tickers that failed |
| `notes` | TEXT | Error summary |

### Useful queries

```sql
-- Latest close prices for all tickers
SELECT ticker, date, close, source
FROM price_history
WHERE date = (SELECT MAX(date) FROM price_history)
ORDER BY ticker;

-- 30-day HV for SPY over the last 3 months
SELECT date, hv_30d
FROM volatility_history
WHERE ticker = 'SPY'
  AND date >= date('now', '-90 days')
ORDER BY date;

-- 10Y-2Y yield spread (recession indicator) over last year
SELECT date, value
FROM macro_data
WHERE series_id = 'T10Y2Y'
  AND date >= date('now', '-365 days')
ORDER BY date;

-- Which tickers used the yfinance fallback?
SELECT ticker, COUNT(*) AS rows
FROM price_history
WHERE source = 'yfinance'
GROUP BY ticker;

-- Review all past pipeline runs
SELECT run_at, status, tickers_ok, tickers_fail, notes
FROM pipeline_log
ORDER BY run_at DESC;
```

---

## 8. Troubleshooting

### `EnvironmentError: POLYGON_API_KEY is not set`
You haven't created `.env`, or it's missing the key.
```bash
copy .env.example .env    # Windows
cp .env.example .env      # macOS/Linux
# then open .env and fill in your key
```

### `Polygon HTTP 403` on OHLCV (not options)
Your API key may be invalid or expired.
Go to [polygon.io/dashboard/api-keys](https://polygon.io/dashboard/api-keys) and regenerate.

### `Polygon rate-limited (429)` with yfinance fallback
Normal on the free plan (5 req/min). The fallback handles it automatically — no action needed.

### `FRED fetch failed: Bad Request ... not a 32 character string`
Your FRED key placeholder is still in `.env`.
Open `.env` and replace `your_fred_api_key_here` with your real key.

### `FileNotFoundError: config.yaml`
Run the script from the project directory:
```bash
cd path/to/amazing-raman
python run_daily.py
```

### `hv_21d` / `hv_30d` / `hv_60d` are NULL for early rows
Expected — rolling windows need 21/30/60 prior trading days before producing values. The first 60 rows after a fresh load will have NULLs for longer windows.

### Gap detected warning persists after rerun
```bash
python run_daily.py --full-reload --tickers AAPL
```

---

## 9. Upgrade Path — Paid Features

### Enable Implied Volatility (ATM 30-day IV)

Requires **Polygon Starter plan** (~$29/month).

1. Upgrade at [polygon.io/dashboard/subscriptions](https://polygon.io/dashboard/subscriptions)
2. **No code changes needed** — the pipeline retries the options endpoint automatically
3. IV populates in `volatility_history.iv_atm_30d`

### Extend history beyond 1 year

Edit `config.yaml`:
```yaml
fetch:
  default_history_days: 1825   # 5 years
```
Then run:
```bash
python run_daily.py --full-reload
```

---

## 10. Daily Workflow

### Every market day

```bash
# 1. Activate virtual environment
venv\Scripts\activate         # Windows
source venv/bin/activate      # macOS/Linux

# 2. Run the update
python run_daily.py
```

### What to check

| Final log line | Status | Action |
|---|---|---|
| `Run complete: 10/10 tickers OK \| macro: OK` | ✅ All good | Nothing |
| `Run complete: 9/10 tickers OK` | ⚠️ Partial | Note which ticker failed; rerun next day |
| Process exits with code 2 | ❌ Fatal | Fix `.env` or `config.yaml` |

### Verify database row counts (quick check)

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/market.db')
for tbl in ['price_history','volatility_history','macro_data','pipeline_log']:
    n = conn.execute(f'SELECT COUNT(*) FROM {tbl}').fetchone()[0]
    print(f'{tbl:25s}: {n:,} rows')
"
```

---

## File Reference

| File | Purpose | Should you edit it? |
|---|---|---|
| `.env` | Your secret API keys | **Yes** — fill in your keys |
| `.env.example` | Template (safe to commit to git) | No |
| `config.yaml` | Tickers, FRED series, all settings | **Yes** — freely edit |
| `requirements.txt` | Python package list | No |
| `data_fetcher.py` | API integration logic | No |
| `database.py` | SQLite schema and helpers | No |
| `run_daily.py` | Main runner script | No |
| `data/market.db` | The database (auto-created) | No |
| `logs/` | Daily log files (auto-created, 30-day retention) | No |
