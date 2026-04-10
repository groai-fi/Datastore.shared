"""
Permanently remove local price data for a Binance symbol.

Deletes the entire symbol directory from the local price root, including
all part.*.parquet files. After removal, the symbol will no longer appear
in `binance-list-symbols` and `binance-auto-update` will skip it.

Safety model
------------
- Without --yes: lists the directory contents and prompts for confirmation
- With --yes:    deletes immediately (for scripted use)
- Deletion is irreversible unless you have a backup

Usage (installed CLI):
    # Interactive (prompts before deleting)
    binance-remove-symbol --symbol SOLUSDT --path /data/prices_v3.parquet

    # Non-interactive
    binance-remove-symbol --symbol SOLUSDT --path /data/prices_v3.parquet --yes

Usage (direct):
    python -m groai_fi_datastore_shared.Binance.cli.remove_symbol --symbol SOLUSDT --path /data/...
"""
import sys
import shutil
import argparse
from pathlib import Path


# ── Argument parsing ─────────────────────────────────────────────────────────

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Permanently remove local price data for a Binance symbol."
    )
    parser.add_argument("--symbol", type=str, required=True,
                        help="Trading pair symbol to remove (e.g. SOLUSDT)")
    parser.add_argument("--path", type=str, required=True,
                        help="Absolute path to the price root directory (e.g. /data/prices_v3.parquet)")
    parser.add_argument("--exchange", type=str, default="Binance",
                        help="Exchange label (default: Binance)")
    parser.add_argument("--yes", action="store_true",
                        help="Skip confirmation prompt (use in scripts)")
    return parser.parse_args()


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    """Main entry point (registered as `binance-remove-symbol` CLI)."""
    args = parse_arguments()
    symbol_dir = Path(args.path) / f"exchange={args.exchange}" / f"symbol={args.symbol}"

    if not symbol_dir.exists():
        print(f"No data found for symbol={args.symbol} at {symbol_dir}")
        print("Nothing to remove.")
        sys.exit(0)

    # ── List files to be deleted ──────────────────────────────────────────────
    all_files = list(symbol_dir.rglob("*"))
    file_list = [f for f in all_files if f.is_file()]

    print(f"\nDirectory to be permanently deleted: {symbol_dir}")
    print(f"Contents ({len(file_list)} file(s)):")
    for f in sorted(file_list):
        print(f"  {f.relative_to(symbol_dir.parent.parent)}")

    # ── Confirmation prompt ───────────────────────────────────────────────────
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
        shutil.rmtree(symbol_dir)
        print(f"\n✓ Deleted {symbol_dir}")
        print(f"  `binance-auto-update` will no longer track {args.symbol}.")
    except Exception as e:
        print(f"✗ Deletion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
