"""
Permanently remove all S3 data for a Binance symbol.

After removal, the symbol will no longer appear in `binance-list-symbols-s3`
output and `binance-auto-update-s3` will not attempt to update it.

Safety model
------------
- Without --yes: lists the objects to be deleted and prompts for confirmation
- With --yes:    deletes immediately (for scripted / cron use)
- Deletions are irreversible — S3 has no recycle bin

Usage (installed CLI):
    # Interactive (prompts before deleting)
    binance-remove-symbol-s3 --symbol SOLUSDT

    # Non-interactive (for scripts)
    binance-remove-symbol-s3 --symbol SOLUSDT --yes

Usage (direct):
    python -m groai_fi_datastore_shared.Binance.cli.remove_symbol_s3 --symbol SOLUSDT

Required env vars:
    S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
"""
import os
import sys
import argparse

from groai_fi_datastore_shared.Binance.utils import readable_error
from groai_fi_datastore_shared.Binance.cli.s3_utils import (
    delete_s3_keys,
    _boto3_client,
)


# ── Argument parsing ─────────────────────────────────────────────────────────

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Permanently remove all S3 data for a Binance symbol."
    )
    parser.add_argument("--symbol", type=str, required=True,
                        help="Trading pair symbol to remove (e.g. SOLUSDT)")
    parser.add_argument("--bucket", type=str,
                        default=os.environ.get("S3_BUCKET_NAME", ""),
                        help="S3 bucket name (default: $S3_BUCKET_NAME)")
    parser.add_argument("--price-root", type=str, default="prices_v3.parquet",
                        help="Root prefix inside the bucket (default: prices_v3.parquet)")
    parser.add_argument("--exchange", type=str, default="Binance",
                        help="Exchange label (default: Binance)")
    parser.add_argument("--yes", action="store_true",
                        help="Skip confirmation prompt (use in scripts/cron)")
    return parser.parse_args()


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    """Main entry point (registered as `binance-remove-symbol-s3` CLI)."""
    args = parse_arguments()

    if not args.bucket:
        print("Error: S3 bucket not specified. Set --bucket or $S3_BUCKET_NAME.")
        sys.exit(1)

    prefix = f"{args.price_root}/exchange={args.exchange}/symbol={args.symbol}/"
    s3_url = f"s3://{args.bucket}/{prefix}"

    # ── List objects to be deleted ────────────────────────────────────────────
    try:
        s3 = _boto3_client()
        keys = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=args.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    except Exception as e:
        err = readable_error(e, __file__)
        print(f"Error listing objects: {err}")
        sys.exit(1)

    if not keys:
        print(f"No data found for symbol={args.symbol} at {s3_url}")
        print("Nothing to remove.")
        sys.exit(0)

    # ── Confirmation prompt ───────────────────────────────────────────────────
    print(f"\nObjects to be permanently deleted from {s3_url}:")
    for k in keys:
        print(f"  {k}")
    print(f"\nTotal: {len(keys)} object(s)")

    if not args.yes:
        try:
            answer = input(
                f"\n⚠  This action is IRREVERSIBLE. "
                f"Type the symbol '{args.symbol}' to confirm deletion: "
            ).strip()
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            sys.exit(0)

        if answer != args.symbol:
            print("Confirmation did not match. Aborted — nothing was deleted.")
            sys.exit(0)

    # ── Delete ────────────────────────────────────────────────────────────────
    try:
        deleted = delete_s3_keys(args.bucket, keys)
        print(f"\n✓ Deleted {deleted} object(s) for symbol={args.symbol}")
        print(f"  `binance-auto-update-s3` will no longer track {args.symbol}.")
    except Exception as e:
        err = readable_error(e, __file__)
        print(f"✗ Deletion failed: {err}")
        sys.exit(1)


if __name__ == "__main__":
    run()
