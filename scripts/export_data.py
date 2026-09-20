"""
export_data.py — Export CLI Script
====================================
CLI to export market data from SQLite to CSV.

Usage
-----
    python scripts/export_data.py --ticker SPY
    python scripts/export_data.py --table price_history
    python scripts/export_data.py --all-tickers
"""

import argparse
import sys
from pathlib import Path

# Add src to pythonpath so sigmafeed is importable
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "src"))

import yaml
from sigmafeed.exporters.exporter import export_ticker_to_csv, export_table_to_csv


def main():
    parser = argparse.ArgumentParser(description="Export SigmaFeed data to CSV.")
    parser.add_argument("--ticker", type=str, help="Ticker to export (e.g. SPY, AAPL)")
    parser.add_argument("--table", type=str, help="Table name to export (e.g. price_history, macro_data)")
    parser.add_argument("--all-tickers", action="store_true", help="Export all configured tickers to CSV")
    parser.add_argument("--db", default="data/market.db", help="Path to SQLite database")
    parser.add_argument("--out", type=str, help="Custom output CSV file path")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config file")

    args = parser.parse_args()

    if args.ticker:
        out = export_ticker_to_csv(args.db, args.ticker.upper(), args.out)
        print(f"Exported {args.ticker.upper()} to {out}")

    elif args.table:
        out = export_table_to_csv(args.db, args.table, args.out)
        print(f"Exported table '{args.table}' to {out}")

    elif args.all_tickers:
        cfg_path = Path(args.config)
        if not cfg_path.exists():
            cfg_path = Path("config.yaml")
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        tickers = cfg.get("tickers", [])
        for t in tickers:
            out = export_ticker_to_csv(args.db, t)
            print(f"Exported {t} to {out}")
    else:
        # Default: export SPY
        out = export_ticker_to_csv(args.db, "SPY", args.out)
        print(f"No option specified. Exported default SPY result to {out}")


if __name__ == "__main__":
    main()
