"""
List all Binance symbols tracked in a local price data directory.

Provides a human-readable inventory of what is stored locally, including
the last recorded date, part count, and merge status for each symbol.

Usage (installed CLI):
    binance-list-symbols --path /data/prices_v3.parquet

Usage (direct):
    python -m groai_fi_datastore_shared.Binance.cli.list_symbols --path /data/prices_v3.parquet

Example output:
    Tracked symbols in /data/prices_v3.parquet
    exchange=Binance (4 symbols)

      BTCUSDT   last=2026-04-10  parts=10  (pending merge)
      ETHUSDT   last=2026-04-10  parts=1   (merged)
      BCHUSDT   last=2026-04-09  parts=23  (pending merge)
      LTCUSDT   last=2026-03-31  parts=1   (merged)
"""
import sys
import argparse
from pathlib import Path

import pandas as pd


# ── Argument parsing ─────────────────────────────────────────────────────────

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="List all Binance symbols tracked in a local price data directory."
    )
    parser.add_argument("--path", type=str, required=True,
                        help="Absolute path to the price root directory (e.g. /data/prices_v3.parquet)")
    parser.add_argument("--exchange", type=str, default="Binance",
                        help="Exchange label (default: Binance)")
    return parser.parse_args()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_last_date(symbol_path: Path):
    """Return the max date from local parquet files, or None."""
    try:
        files = sorted(symbol_path.glob("part.*.parquet"),
                       key=lambda x: x.name)
        if not files:
            return None
        # Read only the last file for speed
        df = pd.read_parquet(files[-1], engine="pyarrow", columns=["date"]
                             if "date" in pd.read_parquet(files[-1], engine="pyarrow").columns
                             else None)
        if "date" in df.columns:
            return df["date"].max()
        if isinstance(df.index, pd.DatetimeIndex):
            return df.index.max()
        return None
    except Exception:
        return None


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    """Main entry point (registered as `binance-list-symbols` CLI)."""
    args = parse_arguments()
    price_root   = Path(args.path)
    exchange_dir = price_root / f"exchange={args.exchange}"

    if not exchange_dir.exists():
        print(f"Error: Exchange directory not found at {exchange_dir}")
        sys.exit(1)

    symbol_dirs = sorted(
        d for d in exchange_dir.iterdir()
        if d.is_dir() and d.name.startswith("symbol=")
    )

    if not symbol_dirs:
        print(f"No symbols found in {exchange_dir}")
        print("Use `binance-download-price --symbol <SYMBOL> --path <PATH>` to add one.")
        sys.exit(0)

    print(f"\nTracked symbols in {price_root}")
    print(f"exchange={args.exchange} ({len(symbol_dirs)} symbol{'s' if len(symbol_dirs) != 1 else ''})\n")

    col_w = max(len(d.name.replace("symbol=", "")) for d in symbol_dirs) + 2

    for symbol_dir in symbol_dirs:
        symbol     = symbol_dir.name.replace("symbol=", "")
        parts      = list(symbol_dir.glob("part.*.parquet"))
        part_count = len(parts)
        last_date  = _get_last_date(symbol_dir)
        last_str   = pd.Timestamp(last_date).strftime("%Y-%m-%d") if last_date is not None else "unknown"

        if part_count == 1:
            status = "(merged)"
        elif part_count > 50:
            status = "(⚠ pending merge)"
        else:
            status = ""

        print(f"  {symbol:<{col_w}}  last={last_str}  parts={part_count:<4}  {status}")

    print()


if __name__ == "__main__":
    run()
