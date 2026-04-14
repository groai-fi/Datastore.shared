"""
Merge all part.*.parquet files for a Binance symbol on S3 into a single
part.00000.parquet, then delete the old parts.

This script is the S3 counterpart of merge_parquet_prices.py.
Instead of Dask, it uses DuckDB's native S3 glob reads and COPY to write
a deduplicated, date-sorted merged file directly in S3 without any local
temporary storage.

Merge lifecycle
---------------
Before merge:
    part.00000.parquet  ← previous merged (or absent on first merge)
    part.1744000000.parquet
    part.1744001000.parquet
    ...

After merge:
    part.00000.parquet  ← single source of truth ✅

The glob part.*.parquet always includes part.00000.parquet (00000 matches *),
so future get_max_date_s3 and download appends work seamlessly.

Safety model
------------
1. Write part.00000.parquet via DuckDB COPY (DISTINCT ON date, ORDER BY date)
2. Validate: count merged rows == count pre-merge rows
3. Only delete old part.<ts>.parquet if validation passes
4. Raise RuntimeError on mismatch — old parts are left untouched
5. --no-delete-parts flag suppresses step 3 (debug opt-out)

Usage (installed CLI):
    binance-merge-parquet-s3 --symbol BTCUSDT

Usage (direct):
    python -m groai_fi_datastore_shared.Binance.cli.merge_parquet_prices_s3 --symbol BTCUSDT

Required env vars:
    S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
"""
import os
import sys
import argparse

import duckdb
import pandas as pd

from groai_fi_datastore_shared.Binance.utils import readable_error
from groai_fi_datastore_shared.Binance.cli.s3_utils import (
    configure_duckdb_s3,
    get_s3_prefix,
    get_s3_glob,
    count_rows_s3,
    list_part_keys,
    delete_s3_keys,
    write_parquet_to_s3,
)


# ── Argument parsing ─────────────────────────────────────────────────────────

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Merge S3 parquet parts for a Binance symbol into one file."
    )
    parser.add_argument("--symbol", type=str, required=True,
                        help="Trading pair symbol (e.g. BTCUSDT)")
    parser.add_argument("--bucket", type=str,
                        default=os.environ.get("S3_BUCKET_NAME", ""),
                        help="S3 bucket name (default: $S3_BUCKET_NAME)")
    parser.add_argument("--price-root", type=str, default="prices_v3.parquet",
                        help="Root prefix inside the bucket (default: prices_v3.parquet)")
    parser.add_argument("--exchange", type=str, default="Binance",
                        help="Exchange label (default: Binance)")
    parser.add_argument("--no-delete-parts", action="store_true",
                        help="Skip deleting old parts after merge (debug opt-out only)")
    return parser.parse_args()


# ── Core merge function ──────────────────────────────────────────────────────

def run_for_symbol(
    symbol: str,
    bucket: str,
    price_root: str,
    exchange: str,
    no_delete_parts: bool = False,
) -> bool:
    """
    Merge all part.*.parquet files for a symbol into part.00000.parquet.

    Returns True on success, False if there was nothing to merge.
    Raises RuntimeError if row-count validation fails after merge.
    """
    con = configure_duckdb_s3(duckdb.connect())
    s3_prefix   = get_s3_prefix(bucket, price_root, exchange, symbol)
    s3_glob     = get_s3_glob(bucket, price_root, exchange, symbol)
    merged_path = f"{s3_prefix}/part.00000.parquet"

    # ── Step 1: Count pre-merge rows ─────────────────────────────────────────
    pre_count = count_rows_s3(con, s3_glob)
    if pre_count == 0:
        print(f"  [{symbol}] No parquet data found at {s3_glob}. Skipping merge.")
        return False

    print(f"  [{symbol}] Pre-merge row count: {pre_count}")

    old_keys = list_part_keys(bucket, price_root, exchange, symbol,
                               exclude="part.00000.parquet")
    print(f"  [{symbol}] Found {len(old_keys)} part files to consolidate")

    # ── Step 2: Write merged file via PyArrow (price_parquet_v3 compliance) ────
    print(f"  [{symbol}] Writing merged file → {merged_path}")

    # Read via DuckDB (fast parallel S3 reads, dedup logic).
    # Explicitly select canonical columns so legacy files with exchange/symbol
    # columns are handled gracefully — those Hive-partition columns are excluded.
    merged_df = con.execute(f"""
        SELECT date, yymm, open, high, low, close, volume
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY date) AS _rn
            FROM read_parquet('{s3_glob}')
        )
        WHERE _rn = 1
        ORDER BY date
    """).df()

    merged_df["date"] = pd.to_datetime(merged_df["date"], utc=True)
    merged_df = merged_df.set_index("date")

    # Write via PyArrow — ensures date-as-index + yymm dict-encoded (spec compliance)
    write_parquet_to_s3(merged_df, merged_path)
    print(f"  [{symbol}] Merged file written")

    # ── Step 3: Validate row count ────────────────────────────────────────────
    post_count = count_rows_s3(con, merged_path)
    print(f"  [{symbol}] Post-merge row count: {post_count}")

    if post_count != pre_count:
        raise RuntimeError(
            f"Merge validation FAILED for {symbol}: "
            f"pre_count={pre_count}, post_count={post_count}. "
            f"Old parts have NOT been deleted. Please inspect {s3_glob}."
        )
    print(f"  [{symbol}] Row count validation passed ✅ ({post_count} rows)")

    # ── Step 4: Delete old parts ──────────────────────────────────────────────
    if no_delete_parts:
        print(f"  [{symbol}] --no-delete-parts set: skipping deletion")
        return True

    if old_keys:
        deleted = delete_s3_keys(bucket, old_keys)
        print(f"  [{symbol}] Deleted {deleted} old part files ✅")
    else:
        print(f"  [{symbol}] No old parts to delete (was already a single merged file)")

    return True


# ── CLI entry point ──────────────────────────────────────────────────────────

def run():
    """Main entry point (registered as `binance-merge-parquet-s3` CLI)."""
    args = parse_arguments()

    if not args.bucket:
        print("Error: S3 bucket not specified. Set --bucket or $S3_BUCKET_NAME.")
        sys.exit(1)

    try:
        success = run_for_symbol(
            symbol=args.symbol,
            bucket=args.bucket,
            price_root=args.price_root,
            exchange=args.exchange,
            no_delete_parts=args.no_delete_parts,
        )
        if success:
            print(f"✓ {args.symbol}: merged to part.00000.parquet")
        else:
            print(f"  {args.symbol}: nothing to merge")
        sys.exit(0)
    except RuntimeError as e:
        print(f"✗ {args.symbol}: {e}")
        sys.exit(1)
    except Exception as e:
        err = readable_error(e, __file__)
        print(f"✗ {args.symbol}: {err}")
        sys.exit(1)


if __name__ == "__main__":
    run()
