"""
List all Binance symbols currently tracked on S3.

Provides a human-readable inventory of what is stored in S3, including
the last recorded date, part count, and merge status for each symbol.

Usage (installed CLI):
    binance-list-symbols-s3

Usage (direct):
    python -m groai_fi_datastore_shared.Binance.cli.list_symbols_s3

Required env vars:
    S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY

Example output:
    Tracked symbols on s3://my-bucket/prices_v3.parquet
    exchange=Binance (5 symbols)

      BTCUSDT   last=2026-04-10  parts=1  (merged)
      ETHUSDT   last=2026-04-10  parts=1  (merged)
      BCHUSDT   last=2026-04-09  parts=23 (pending merge)
      SOLUSDT   last=2026-04-08  parts=5
      LTCUSDT   last=2026-03-31  parts=1  (merged)
"""
import os
import sys
import argparse

import duckdb

from groai_fi_datastore_shared.Binance.utils import readable_error
from groai_fi_datastore_shared.Binance.cli.s3_utils import (
    configure_duckdb_s3,
    list_s3_symbols,
    get_s3_glob,
    get_max_date_s3,
    count_parts_s3,
)


# ── Argument parsing ─────────────────────────────────────────────────────────

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="List all Binance symbols tracked on S3 with status info."
    )
    parser.add_argument("--bucket", type=str,
                        default=os.environ.get("S3_BUCKET_NAME", ""),
                        help="S3 bucket name (default: $S3_BUCKET_NAME)")
    parser.add_argument("--price-root", type=str, default="prices_v3.parquet",
                        help="Root prefix inside the bucket (default: prices_v3.parquet)")
    parser.add_argument("--exchange", type=str, default="Binance",
                        help="Exchange label (default: Binance)")
    return parser.parse_args()


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    """Main entry point (registered as `binance-list-symbols-s3` CLI)."""
    args = parse_arguments()

    if not args.bucket:
        print("Error: S3 bucket not specified. Set --bucket or $S3_BUCKET_NAME.")
        sys.exit(1)

    try:
        symbols = list_s3_symbols(args.bucket, args.price_root, args.exchange)
    except Exception as e:
        err = readable_error(e, __file__)
        print(f"Error listing symbols: {err}")
        sys.exit(1)

    if not symbols:
        print(f"No symbols found on s3://{args.bucket}/{args.price_root}/exchange={args.exchange}/")
        print("Use `binance-download-price-s3 --symbol <SYMBOL>` to add a new symbol.")
        sys.exit(0)

    print(f"\nTracked symbols on s3://{args.bucket}/{args.price_root}")
    print(f"exchange={args.exchange} ({len(symbols)} symbol{'s' if len(symbols) != 1 else ''})\n")

    col_w = max(len(s) for s in symbols) + 2
    con = configure_duckdb_s3(duckdb.connect())

    for symbol in symbols:
        s3_glob    = get_s3_glob(args.bucket, args.price_root, args.exchange, symbol)
        last_date  = get_max_date_s3(con, s3_glob)
        part_count = count_parts_s3(args.bucket, args.price_root, args.exchange, symbol)

        last_str   = last_date.strftime("%Y-%m-%d") if last_date else "unknown"

        # Determine merge status
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
