"""
Download Binance price data directly to S3 via DuckDB.

This script is the S3 counterpart of download_price_binance.py.

Key difference from the local script
-------------------------------------
Both scripts call the same pure-API fetcher:
    BinanceMarketDataDownloader.download_data_from_binance_1minute()

That function contacts the Binance REST API and returns a plain pandas
DataFrame — it has no knowledge of local paths or S3. Only the *save*
step differs:
  - download_price_binance.py    → saves locally via Dask / pyarrow
  - download_price_binance_s3.py → uploads to S3 via DuckDB COPY

Usage (installed CLI):
    binance-download-price-s3 --symbol BTCUSDT --start-date 2024/01/01

Usage (direct):
    python -m groai_fi_datastore_shared.Binance.cli.download_price_binance_s3 --symbol BTCUSDT ...

Required env vars:
    S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
    BINANCE_API_KEY, BINANCE_API_SECRET
"""
import os
import sys
import time
import argparse
from datetime import datetime, timedelta, timezone

import duckdb
import pandas as pd

from groai_fi_datastore_shared.Binance import schema
from groai_fi_datastore_shared.Binance.BinanceMarketDataDownloader import (
    download_data_from_binance_1minute,
)
from groai_fi_datastore_shared.Binance.utils import readable_error
from groai_fi_datastore_shared.Binance.cli.s3_utils import (
    configure_duckdb_s3,
    get_s3_prefix,
    get_s3_glob,
    get_max_date_s3,
)


# ── Argument parsing ─────────────────────────────────────────────────────────

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Download Binance price data and upload to S3 via DuckDB."
    )
    parser.add_argument("--symbol", type=str, required=True,
                        help="Trading pair symbol (e.g. BTCUSDT)")
    parser.add_argument("--tframe", type=str, default="1m",
                        help="Kline timeframe (default: 1m)")
    parser.add_argument("--bucket", type=str,
                        default=os.environ.get("S3_BUCKET_NAME", ""),
                        help="S3 bucket name (default: $S3_BUCKET_NAME)")
    parser.add_argument("--price-root", type=str, default="prices_v3.parquet",
                        help="Root prefix inside the bucket (default: prices_v3.parquet)")
    parser.add_argument("--start-date", type=str, default="2018/03/01",
                        help="Fallback start date YYYY/MM/DD — used only when no S3 data exists")
    parser.add_argument("--exchange", type=str, default="Binance",
                        help="Exchange label (default: Binance)")
    return parser.parse_args()


# ── DataFrame shaping ────────────────────────────────────────────────────────

def _shape_df(df: pd.DataFrame, symbol: str, exchange: str) -> pd.DataFrame:
    """
    Trim the raw klines DataFrame to the canonical price schema and add
    the metadata columns required by the parquet layout.
    """
    df = df.iloc[:, :6].copy()
    df.columns = schema.price_columns      # date, open, high, low, close, volume

    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True)

    df["symbol"] = symbol
    df["exchange"] = exchange
    df["yymm"] = df["date"].dt.strftime("%y%m")

    for col, dtype in schema.price_parquet.items():
        if col in df.columns and col != "date":
            df[col] = df[col].astype(dtype)

    df = df[[c for c in schema.price_header_parquet if c in df.columns]]
    df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


# ── Core download function ───────────────────────────────────────────────────

def run_for_symbol(
    symbol: str,
    tframe: str,
    bucket: str,
    price_root: str,
    start_date_fallback: datetime,
    exchange: str,
) -> int:
    """
    Download price data for one symbol and upload it to S3 as a new
    part.<unix_ts>.parquet file.

    Returns the number of rows uploaded (0 if nothing new).
    """
    con = configure_duckdb_s3(duckdb.connect())
    s3_prefix = get_s3_prefix(bucket, price_root, exchange, symbol)
    s3_glob   = get_s3_glob(bucket, price_root, exchange, symbol)

    existing_max = get_max_date_s3(con, s3_glob)
    if existing_max is not None:
        start = existing_max + timedelta(minutes=1)
        print(f"  [{symbol}] Resuming from S3 MAX(date): {existing_max} → start: {start}")
    else:
        start = start_date_fallback.replace(tzinfo=timezone.utc) \
            if start_date_fallback.tzinfo is None else start_date_fallback
        print(f"  [{symbol}] No S3 data found; starting from {start}")

    now = datetime.now(tz=timezone.utc) + timedelta(minutes=1)
    if start >= now:
        print(f"  [{symbol}] Already up to date")
        return 0

    from_date_str = start.strftime(schema.datetimefmt)
    to_date_str   = now.strftime(schema.datetimefmt)

    print(f"  [{symbol}] Downloading {from_date_str} → {to_date_str}")

    import logging
    _null_logger = logging.getLogger(f"binance.s3.{symbol}")
    _null_logger.addHandler(logging.NullHandler())

    df = download_data_from_binance_1minute(
        symbol=symbol,
        kline_tframe=tframe,
        from_date=from_date_str,
        to_date=to_date_str,
        logger=_null_logger,
        step=8,
    )

    if df is None or df.empty:
        print(f"  [{symbol}] No new data returned")
        return 0

    df = _shape_df(df, symbol, exchange)
    row_count = len(df)

    ts = int(time.time())
    s3_dest = f"{s3_prefix}/part.{ts}.parquet"
    con.register("df_view", df)
    con.execute(
        f"COPY df_view TO '{s3_dest}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
    )
    print(f"  [{symbol}] Uploaded {row_count} rows → {s3_dest}")
    return row_count


# ── CLI entry point ──────────────────────────────────────────────────────────

def run():
    """Main entry point (registered as `binance-download-price-s3` CLI)."""
    args = parse_arguments()

    if not args.bucket:
        print("Error: S3 bucket not specified. Set --bucket or $S3_BUCKET_NAME.")
        sys.exit(1)

    try:
        start_date_fallback = datetime.strptime(args.start_date, "%Y/%m/%d")
    except ValueError:
        print(f"Error: Invalid --start-date format '{args.start_date}'. Use YYYY/MM/DD.")
        sys.exit(1)

    try:
        rows = run_for_symbol(
            symbol=args.symbol,
            tframe=args.tframe,
            bucket=args.bucket,
            price_root=args.price_root,
            start_date_fallback=start_date_fallback,
            exchange=args.exchange,
        )
        print(f"✓ {args.symbol}: {rows} rows uploaded to S3")
        sys.exit(0)
    except Exception as e:
        err = readable_error(e, __file__)
        print(f"✗ {args.symbol}: {err}")
        sys.exit(1)


if __name__ == "__main__":
    run()
