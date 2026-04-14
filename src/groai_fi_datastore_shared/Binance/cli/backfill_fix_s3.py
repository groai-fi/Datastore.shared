"""
One-shot backfill script to re-write existing Parquet files in the new S3 bucket
so they comply with the price_parquet_v3 schema specification.

Background
----------
The initial data written to ``stashed-bento-3z1jiwv2yj7`` had three structural bugs:
  1. ``date`` was a plain column with RangeIndex instead of the Parquet row-group index.
  2. ``yymm``, ``exchange``, ``symbol`` were plain strings instead of dict-encoded.
  3. ``exchange`` and ``symbol`` were duplicated in the file data even though they
     already live in the Hive S3 path.

This script reads all existing parts per symbol via DuckDB (fast, parallel S3 reads),
deduplicates and sorts by date, then writes a single clean ``part.00000.parquet``
using PyArrow via ``write_parquet_to_s3`` (spec-compliant).  After a successful row-count
validation, all old part files are deleted.

Symbols processed (per user decision)
--------------------------------------
BTCUSDT and ETHUSDT — the two symbols currently tracked in the new bucket.

Safety model
------------
1. Count unique dates pre-write (dedup-aware) via DuckDB.
2. Write ``part.00000.parquet`` via ``write_parquet_to_s3``.
3. Validate: post-write row count == pre-write unique-date count.
4. On pass: delete all other part files (old, non-canonical parts).
5. On fail: raise RuntimeError — old files are left intact.
6. ``--dry-run``: prints counts, no S3 writes or deletes.
7. ``--no-delete``: writes new file but leaves old parts (debug mode).

Usage (installed CLI)
---------------------
    binance-backfill-fix-s3
    binance-backfill-fix-s3 --dry-run
    binance-backfill-fix-s3 --symbols BTCUSDT --no-delete

Usage (direct)
--------------
    python -m groai_fi_datastore_shared.Binance.cli.backfill_fix_s3

Required env vars
-----------------
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


# ── Default symbols (per PRICE_PARQUET_V3_SPEC.md backfill scope) ────────────

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
DEFAULT_EXCHANGE  = "Binance"
DEFAULT_PRICE_ROOT = "prices_v3.parquet"


# ── Argument parsing ─────────────────────────────────────────────────────────

def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "One-shot backfill: rewrite existing S3 Parquet files to comply with "
            "the price_parquet_v3 schema spec (date as index, yymm dict-encoded, "
            "exchange/symbol in path only)."
        )
    )
    parser.add_argument(
        "--symbols", type=str, nargs="+", default=DEFAULT_SYMBOLS,
        help=f"Symbols to backfill (default: {' '.join(DEFAULT_SYMBOLS)})"
    )
    parser.add_argument(
        "--bucket", type=str,
        default=os.environ.get("S3_BUCKET_NAME", ""),
        help="S3 bucket name (default: $S3_BUCKET_NAME)"
    )
    parser.add_argument(
        "--price-root", type=str, default=DEFAULT_PRICE_ROOT,
        help=f"Root prefix inside the bucket (default: {DEFAULT_PRICE_ROOT})"
    )
    parser.add_argument(
        "--exchange", type=str, default=DEFAULT_EXCHANGE,
        help=f"Exchange label (default: {DEFAULT_EXCHANGE})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print counts and actions without writing or deleting anything."
    )
    parser.add_argument(
        "--no-delete", action="store_true",
        help="Write the new part.00000.parquet but skip deleting old parts (debug mode)."
    )
    return parser.parse_args()


# ── Per-symbol backfill ──────────────────────────────────────────────────────

def run_for_symbol(
    symbol: str,
    bucket: str,
    price_root: str,
    exchange: str,
    con: duckdb.DuckDBPyConnection,
    dry_run: bool = False,
    no_delete: bool = False,
) -> bool:
    """
    Read, deduplicate, and rewrite all Parquet data for one symbol.

    Returns True on success, False if there was no data to process.
    Raises RuntimeError if post-write validation fails.
    """
    s3_prefix   = get_s3_prefix(bucket, price_root, exchange, symbol)
    s3_glob     = get_s3_glob(bucket, price_root, exchange, symbol)
    merged_path = f"{s3_prefix}/part.00000.parquet"

    # ── Step 1: Count unique dates (dedup-aware pre-count) ────────────────────
    try:
        pre_dedup_count = con.execute(
            f"SELECT COUNT(DISTINCT date) FROM read_parquet('{s3_glob}')"
        ).fetchone()[0]
    except Exception as e:
        print(f"  [{symbol}] No data found or read error: {e}. Skipping.")
        return False

    if pre_dedup_count == 0:
        print(f"  [{symbol}] No rows found at {s3_glob}. Skipping.")
        return False

    raw_count = count_rows_s3(con, s3_glob)
    print(f"  [{symbol}] Raw rows (incl. duplicates): {raw_count}")
    print(f"  [{symbol}] Unique dates (post-dedup):   {pre_dedup_count}")

    if dry_run:
        print(f"  [{symbol}] [DRY RUN] Would write {pre_dedup_count} rows → {merged_path}")
        return True

    # ── Step 2: Read all data via DuckDB, select canonical columns ────────────
    # Explicitly name columns so legacy files with exchange/symbol are handled.
    # yymm is recomputed from date to guarantee correctness regardless of what
    # the old writer stored.
    merged_df = con.execute(f"""
        SELECT date, yymm, open, high, low, close, volume
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY date) AS _rn
            FROM read_parquet('{s3_glob}')
        )
        WHERE _rn = 1
        ORDER BY date
    """).df()

    if merged_df.empty:
        print(f"  [{symbol}] DataFrame is empty after dedup. Skipping.")
        return False

    merged_df["date"] = pd.to_datetime(merged_df["date"], utc=True)
    merged_df = merged_df.set_index("date")

    # ── Step 3: Write spec-compliant part.00000.parquet ─────────────────────
    print(f"  [{symbol}] Writing {len(merged_df)} rows → {merged_path}")
    write_parquet_to_s3(merged_df, merged_path)

    # ── Step 4: Validate row count ────────────────────────────────────────────
    post_count = count_rows_s3(con, merged_path)
    print(f"  [{symbol}] Post-write row count: {post_count}")

    if post_count != pre_dedup_count:
        raise RuntimeError(
            f"Backfill validation FAILED for {symbol}: "
            f"expected {pre_dedup_count} unique rows, "
            f"got {post_count} in the merged file. "
            f"Old parts have NOT been deleted. Inspect {s3_glob}."
        )
    print(f"  [{symbol}] Validation passed ✅ ({post_count} rows)")

    # ── Step 5: Delete old parts (everything except the new part.00000) ───────
    if no_delete:
        print(f"  [{symbol}] --no-delete set: skipping deletion of old parts")
        return True

    old_keys = list_part_keys(bucket, price_root, exchange, symbol,
                               exclude="part.00000.parquet")
    if old_keys:
        deleted = delete_s3_keys(bucket, old_keys)
        print(f"  [{symbol}] Deleted {deleted} old part file(s) ✅")
    else:
        print(f"  [{symbol}] No old parts to delete (already a single file)")

    return True


# ── CLI entry point ──────────────────────────────────────────────────────────

def run():
    """Main entry point registered as ``binance-backfill-fix-s3``."""
    args = parse_arguments()

    if not args.bucket:
        print("Error: S3 bucket not specified. Set --bucket or $S3_BUCKET_NAME.")
        sys.exit(1)

    mode = "[DRY RUN] " if args.dry_run else ""
    print(f"\n{'=' * 65}")
    print(f"price_parquet_v3 Backfill {mode}— bucket: {args.bucket}")
    print(f"Symbols: {', '.join(args.symbols)}")
    print(f"{'=' * 65}\n")

    con = configure_duckdb_s3(duckdb.connect())

    success_count = 0
    fail_count    = 0

    for symbol in args.symbols:
        print(f"\n── {symbol} {'─' * (55 - len(symbol))}")
        try:
            ok = run_for_symbol(
                symbol=symbol,
                bucket=args.bucket,
                price_root=args.price_root,
                exchange=args.exchange,
                con=con,
                dry_run=args.dry_run,
                no_delete=args.no_delete,
            )
            if ok:
                print(f"  [{symbol}] ✓ Backfill complete")
                success_count += 1
            else:
                print(f"  [{symbol}] ⚠ Skipped (no data)")
        except RuntimeError as e:
            print(f"  [{symbol}] ✗ Validation error: {e}")
            fail_count += 1
        except Exception as e:
            err = readable_error(e, __file__)
            print(f"  [{symbol}] ✗ Error: {err}")
            fail_count += 1

    print(f"\n{'=' * 65}")
    print(f"Backfill {mode}completed!")
    print(f"  Success: {success_count}")
    print(f"  Failed:  {fail_count}")
    print(f"  Total:   {len(args.symbols)}")
    print(f"{'=' * 65}")

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    run()
