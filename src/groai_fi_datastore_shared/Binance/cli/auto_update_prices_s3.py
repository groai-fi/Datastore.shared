"""
Auto-update all Binance symbols tracked on S3.

Discovers every symbol present on S3 (via boto3 prefix listing), finds
the last recorded date for each (via DuckDB MAX query), downloads the gap
from the Binance API, and merges when the part count exceeds a threshold.

This is the S3 counterpart of auto_update_prices.py.

Relationship to other scripts
-------------------------------
  download_price_binance_s3.run_for_symbol()  ← called in-process (no subprocess)
  merge_parquet_prices_s3.run_for_symbol()    ← called in-process (no subprocess)

Symbol discovery
-----------------
Symbols are discovered by listing S3 prefixes — no local filesystem or config
file is needed. Adding a new symbol is done by running:
    binance-download-price-s3 --symbol NEWTOKEN --start-date YYYY/MM/DD
After that, this script will automatically include it on the next run.

Usage (installed CLI):
    binance-auto-update-s3

Usage (direct):
    python -m groai_fi_datastore_shared.Binance.cli.auto_update_prices_s3

Required env vars:
    S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
    BINANCE_API_KEY, BINANCE_API_SECRET
"""
import os
import sys
import argparse
from datetime import datetime

from groai_fi_datastore_shared.Binance.utils import readable_error
from groai_fi_datastore_shared.Binance.cli.s3_utils import (
    list_s3_symbols,
    count_parts_s3,
)
from groai_fi_datastore_shared.Binance.cli.download_price_binance_s3 import (
    run_for_symbol as download_symbol,
)
from groai_fi_datastore_shared.Binance.cli.merge_parquet_prices_s3 import (
    run_for_symbol as merge_symbol,
)


# ── Argument parsing ─────────────────────────────────────────────────────────

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Auto-update all Binance symbols tracked on S3."
    )
    parser.add_argument("--bucket", type=str,
                        default=os.environ.get("S3_BUCKET_NAME", ""),
                        help="S3 bucket name (default: $S3_BUCKET_NAME)")
    parser.add_argument("--price-root", type=str, default="prices_v3.parquet",
                        help="Root prefix inside the bucket (default: prices_v3.parquet)")
    parser.add_argument("--tframe", type=str, default="1m",
                        help="Kline timeframe (default: 1m)")
    parser.add_argument("--exchange", type=str, default="Binance",
                        help="Exchange label (default: Binance)")
    parser.add_argument("--merge-threshold", type=int, default=50,
                        help="Part count that triggers a merge (default: 50)")
    parser.add_argument("--no-delete-parts", action="store_true",
                        help="Skip deleting old parts after merge (debug opt-out, passed to merge)")
    return parser.parse_args()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    """Main entry point (registered as `binance-auto-update-s3` CLI)."""
    args = parse_arguments()

    if not args.bucket:
        print("Error: S3 bucket not specified. Set --bucket or $S3_BUCKET_NAME.")
        sys.exit(1)

    # ── Step 1: Discover symbols ──────────────────────────────────────────────
    print(f"\nDiscovering symbols on s3://{args.bucket}/{args.price_root}/exchange={args.exchange}/...")
    try:
        symbols = list_s3_symbols(args.bucket, args.price_root, args.exchange)
    except Exception as e:
        err = readable_error(e, __file__)
        print(f"Error: Symbol discovery failed: {err}")
        sys.exit(1)

    if not symbols:
        print("No symbols found on S3. Use `binance-download-price-s3` to add a symbol first.")
        sys.exit(0)

    print(f"Found {len(symbols)} symbol(s): {', '.join(symbols[:10])}", end="")
    if len(symbols) > 10:
        print(f" … and {len(symbols) - 10} more")
    else:
        print()

    # ── Step 2: Process each symbol ───────────────────────────────────────────
    success_count = 0
    fail_count    = 0

    for symbol in symbols:
        print(f"\n{'=' * 60}")
        print(f"Processing {symbol}")
        print(f"{'=' * 60}")

        # Download
        try:
            rows = download_symbol(
                symbol=symbol,
                tframe=args.tframe,
                bucket=args.bucket,
                price_root=args.price_root,
                start_date_fallback=datetime(2018, 3, 1),
                exchange=args.exchange,
            )
            print(f"  Downloaded: {rows} new rows")
        except Exception as e:
            err = readable_error(e, __file__)
            print(f"  ✗ Download failed: {err}")
            fail_count += 1
            continue

        # Merge if needed
        try:
            part_count = count_parts_s3(args.bucket, args.price_root, args.exchange, symbol)
            print(f"  Part count: {part_count} (threshold: {args.merge_threshold})")

            if part_count > args.merge_threshold:
                print("  Merging...")
                merge_symbol(
                    symbol=symbol,
                    bucket=args.bucket,
                    price_root=args.price_root,
                    exchange=args.exchange,
                    no_delete_parts=args.no_delete_parts,
                )
                print(f"  ✓ Merged → part.00000.parquet")
            else:
                print(f"  Skipping merge (below threshold)")
        except Exception as e:
            err = readable_error(e, __file__)
            print(f"  ✗ Merge failed: {err}")
            fail_count += 1
            continue

        print(f"  ✓ {symbol} updated successfully")
        success_count += 1

    # ── Step 3: Summary ───────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("Auto update completed!")
    print(f"  Success: {success_count}")
    print(f"  Failed:  {fail_count}")
    print(f"  Total:   {len(symbols)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
