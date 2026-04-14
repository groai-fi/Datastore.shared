"""
Shared S3 utilities for Binance S3 CLI scripts.

All functions read credentials from environment variables:
    S3_ENDPOINT_URL      e.g. https://t3.storageapi.dev
    S3_BUCKET_NAME       e.g. stashed-bento-3z1jiwv2yj7
    S3_ACCESS_KEY_ID
    S3_SECRET_ACCESS_KEY
"""
import os
from datetime import datetime, timezone
from typing import Any, Optional


# ── DuckDB configuration ─────────────────────────────────────────────────────

def configure_duckdb_s3(con: Any) -> Any:
    """
    Load the httpfs extension and configure S3-compatible credentials.
    Reads endpoint / key / secret from environment variables.
    """
    import duckdb  # lazy import — only required when S3 operations are performed
    endpoint_raw = os.environ["S3_ENDPOINT_URL"]
    # Strip scheme so DuckDB receives only the host (e.g. t3.storageapi.dev)
    endpoint = endpoint_raw.replace("https://", "").replace("http://", "").rstrip("/")

    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{endpoint}';
        SET s3_access_key_id='{os.environ["S3_ACCESS_KEY_ID"]}';
        SET s3_secret_access_key='{os.environ["S3_SECRET_ACCESS_KEY"]}';
        SET s3_region='auto';
        SET s3_url_style='path';
    """)
    return con


# ── Path helpers ─────────────────────────────────────────────────────────────

def get_s3_prefix(bucket: str, price_root: str, exchange: str, symbol: str) -> str:
    """
    Return the canonical S3 prefix for a symbol.
    e.g. s3://my-bucket/prices_v3.parquet/exchange=Binance/symbol=BTCUSDT
    """
    return f"s3://{bucket}/{price_root}/exchange={exchange}/symbol={symbol}"


def get_s3_glob(bucket: str, price_root: str, exchange: str, symbol: str) -> str:
    """Return a glob that matches all part files for a symbol."""
    return get_s3_prefix(bucket, price_root, exchange, symbol) + "/part.*.parquet"


# ── Symbol discovery ─────────────────────────────────────────────────────────

def list_s3_symbols(bucket: str, price_root: str, exchange: str) -> list:
    """
    Return a sorted list of symbol names tracked under the given exchange prefix.
    Uses boto3 list_objects_v2 with a delimiter to find symbol= sub-prefixes.
    """
    s3 = _boto3_client()
    prefix = f"{price_root}/exchange={exchange}/"
    paginator = s3.get_paginator("list_objects_v2")
    symbols = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder = cp["Prefix"].rstrip("/").split("/")[-1]
            if folder.startswith("symbol="):
                symbols.add(folder.replace("symbol=", ""))
    return sorted(symbols)


# ── Date and count queries ───────────────────────────────────────────────────

def get_max_date_s3(con: Any, s3_glob: str) -> Optional[datetime]:
    """
    Return the maximum date found across all parquet files matching s3_glob.
    Returns None if no files exist or all files are empty.
    """
    try:
        row = con.execute(
            f"SELECT MAX(date) FROM read_parquet('{s3_glob}')"
        ).fetchone()
        val = row[0] if row else None
        if val is None:
            return None
        if isinstance(val, datetime):
            return val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val.astimezone(timezone.utc)
        # Fallback: integer nanosecond timestamp
        return datetime.fromtimestamp(float(val) / 1e9, tz=timezone.utc)
    except Exception:
        return None


def count_rows_s3(con: Any, s3_glob: str) -> int:
    """
    Return the total row count across all parquet files matching s3_glob.
    Used for merge validation.
    """
    try:
        row = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{s3_glob}')"
        ).fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def count_parts_s3(bucket: str, price_root: str, exchange: str, symbol: str) -> int:
    """
    Return the number of part.*.parquet objects for a symbol via boto3.
    Does not require a DuckDB connection.
    """
    s3 = _boto3_client()
    prefix = f"{price_root}/exchange={exchange}/symbol={symbol}/"
    count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            fname = key.split("/")[-1]
            if fname.startswith("part.") and fname.endswith(".parquet"):
                count += 1
    return count


# ── Deletion helpers ─────────────────────────────────────────────────────────

def list_part_keys(bucket: str, price_root: str, exchange: str, symbol: str,
                   exclude: Optional[str] = None) -> list:
    """
    Return a list of S3 keys for all part.*.parquet objects under a symbol prefix.
    Optionally exclude one filename (e.g. 'part.00000.parquet' after merge).
    """
    s3 = _boto3_client()
    prefix = f"{price_root}/exchange={exchange}/symbol={symbol}/"
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            fname = key.split("/")[-1]
            if fname.startswith("part.") and fname.endswith(".parquet"):
                if exclude and fname == exclude:
                    continue
                keys.append(key)
    return keys


def delete_s3_keys(bucket: str, keys: list) -> int:
    """
    Batch-delete a list of S3 keys (1 000 keys per request, as per S3 API limit).
    Prints progress to stdout. Returns the number of keys successfully deleted.
    """
    if not keys:
        return 0

    s3 = _boto3_client()
    deleted = 0
    batch_size = 1000
    for i in range(0, len(keys), batch_size):
        batch = keys[i: i + batch_size]
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        errors = response.get("Errors", [])
        for err in errors:
            print(f"  [ERROR] Failed to delete {err['Key']}: {err['Message']}")
        batch_deleted = len(batch) - len(errors)
        deleted += batch_deleted
        print(f"  Deleted batch of {batch_deleted} objects")
    return deleted


def delete_s3_prefix(bucket: str, prefix: str) -> int:
    """
    Delete ALL S3 objects whose key starts with prefix.
    Used by remove_symbol_s3 to wipe an entire symbol.
    Returns total deleted count.
    """
    s3 = _boto3_client()
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return delete_s3_keys(bucket, keys)


# ── Canonical S3 Parquet writer ──────────────────────────────────────────────

def write_parquet_to_s3(df: "pd.DataFrame", s3_path: str) -> None:
    """
    Write a price DataFrame to S3 as a price_parquet_v3-compliant Parquet file.

    Enforces the full schema contract (see PRICE_PARQUET_V3_SPEC.md):
      - ``date`` must already be the DataFrame index (UTC-aware timestamp).
      - ``yymm`` is dictionary-encoded (BYTE_ARRAY + RLE_DICTIONARY).
      - ``exchange`` and ``symbol`` must NOT be in df.columns (Hive path encodes them).
      - Compression: Snappy.

    Parameters
    ----------
    df : pd.DataFrame
        Price data with ``date`` set as the index.  Must not contain
        ``exchange`` or ``symbol`` columns.
    s3_path : str
        Full S3 URI, e.g.
        ``s3://bucket/prices_v3.parquet/exchange=Binance/symbol=BTCUSDT/part.00000.parquet``
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    import s3fs

    # Guard: confirm spec contract before touching S3
    assert df.index.name == "date", (
        f"write_parquet_to_s3: DataFrame index must be named 'date', "
        f"got '{df.index.name}'. Call df.set_index('date') first."
    )
    for hive_col in ("exchange", "symbol"):
        assert hive_col not in df.columns, (
            f"write_parquet_to_s3: '{hive_col}' must NOT be a file column "
            f"(it belongs in the Hive S3 path). Drop it before writing."
        )

    # Normalise yymm to plain str so PyArrow encodes it as utf8 not large_string
    working_df = df.copy()
    if "yymm" in working_df.columns:
        working_df["yymm"] = working_df["yymm"].astype(str)

    # Build PyArrow table — preserve_index=True stores date as a Parquet column
    # with pandas index metadata so readers restore it as the index automatically
    table = pa.Table.from_pandas(working_df, preserve_index=True)

    # Dictionary-encode yymm (low-cardinality column — dramatically reduces file size)
    if "yymm" in table.schema.names:
        idx = table.schema.get_field_index("yymm")
        yymm_utf8 = table.column("yymm").cast(pa.utf8())   # utf8 not large_string
        encoded = pc.dictionary_encode(yymm_utf8)
        table = table.set_column(idx, pa.field("yymm", encoded.type), encoded)

    fs = s3fs.S3FileSystem(
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        key=os.environ["S3_ACCESS_KEY_ID"],
        secret=os.environ["S3_SECRET_ACCESS_KEY"],
    )

    # s3fs expects bucket/key — strip the s3:// scheme
    path = s3_path.removeprefix("s3://")
    pq.write_table(table, path, filesystem=fs, compression="snappy")


# ── Internal ─────────────────────────────────────────────────────────────────

def _boto3_client():
    """Return a boto3 S3 client configured from environment variables."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    )
