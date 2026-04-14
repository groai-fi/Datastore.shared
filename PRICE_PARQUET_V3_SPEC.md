# price\_parquet\_v3 Schema Specification

> **Antigravity rule — enforced by AI assistant.**  
> Before writing any code that reads from or writes to `s3://<bucket>/prices_v3.parquet/`,
> check every rule in this document. A violation is a data bug, not a style issue.

---

## Purpose

This document is the **authoritative schema contract** for all Parquet files written to the
`prices_v3.parquet` prefix in any GroAI.fi S3 bucket (current: `stashed-bento-3z1jiwv2yj7`).

Writers **MUST** conform to every rule below. Violations break downstream services:
ATR estimator, backtester, DuckDB resampling queries, and `read_data`.

---

## S3 Layout

```
s3://<bucket>/prices_v3.parquet/
  exchange=Binance/
    symbol=BTCUSDT/
      part.00000.parquet   ← merged canonical file
      part.<unix_ts>.parquet  ← incremental append (merged periodically)
    symbol=ETHUSDT/
      ...
```

---

## Hive Partitioning Rule

| Column     | Location           | Rule                                                 |
|------------|--------------------|------------------------------------------------------|
| `exchange` | **S3 path ONLY**   | **MUST NOT** appear as a column inside the Parquet file |
| `symbol`   | **S3 path ONLY**   | **MUST NOT** appear as a column inside the Parquet file |

**Why?** Every row in `exchange=Binance/symbol=BTCUSDT/` has identical `exchange`/`symbol`
values — storing them in the file is 100% redundant. PyArrow, DuckDB, and Dask reconstruct
Hive partition columns from the path at read time. Including them in the file causes a
schema violation and doubles storage for those columns.

---

## File Data Schema

| Column   | Parquet Physical Type                       | Logical Type             | Index? |
|----------|---------------------------------------------|--------------------------|--------|
| `date`   | `INT64`                                     | `TIMESTAMP(UTC, μs)`     | ✅ YES |
| `yymm`   | `BYTE_ARRAY` + `RLE_DICTIONARY` encoding    | `STRING` (dict-encoded)  | ❌ No  |
| `open`   | `DOUBLE`                                    | —                        | ❌ No  |
| `high`   | `DOUBLE`                                    | —                        | ❌ No  |
| `low`    | `DOUBLE`                                    | —                        | ❌ No  |
| `close`  | `DOUBLE`                                    | —                        | ❌ No  |
| `volume` | `DOUBLE`                                    | —                        | ❌ No  |

---

## Rules

### Rule 1 — `date` MUST be the Parquet row-group index

Writers must call `df.set_index("date")` (pandas) **before** passing the DataFrame to
the write function. A `RangeIndex` in the Parquet metadata is a write bug.

```python
# ✅ Correct
df = df.set_index("date")
write_parquet_to_s3(df, s3_path)

# ❌ Wrong — date is a regular column
df.reset_index(drop=True)
pq.write_table(pa.Table.from_pandas(df), ...)
```

### Rule 2 — `yymm` MUST be dictionary-encoded

Use `pyarrow.compute.dictionary_encode()` when building the PyArrow table.
DuckDB `COPY` does not produce dictionary encoding — use `pq.write_table()` via
`write_parquet_to_s3()` instead.

```python
# ✅ Correct (handled automatically by write_parquet_to_s3)
write_parquet_to_s3(df, s3_path)

# ❌ Wrong — DuckDB COPY writes plain string
con.execute(f"COPY df_view TO '{dest}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
```

### Rule 3 — `exchange` and `symbol` MUST NOT be written as file columns

```python
# ✅ Correct — they only live in the S3 path
df = df[schema.price_parquet_file_columns]   # yymm, open, high, low, close, volume
df = df.set_index("date")

# ❌ Wrong — these are Hive partition columns
df["exchange"] = "Binance"
df["symbol"] = "BTCUSDT"
```

### Rule 4 — UTC timezone MUST be explicit

`date` must be `TIMESTAMP(isAdjustedToUTC=true, unit=MICROS)`.  
Naive / timezone-less timestamps are a schema violation.

```python
# ✅ Correct
df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True)

# ❌ Wrong — naive timestamp with no timezone
df["date"] = pd.to_datetime(df["date"], unit="ms")   # no utc=True
```

### Rule 5 — Compression is Snappy

```python
# ✅ Correct
pq.write_table(table, path, filesystem=fs, compression="snappy")

# ❌ Wrong
pq.write_table(table, path, filesystem=fs)  # default is no compression
```

### Rule 6 — No duplicate timestamps per symbol

Each `date` value must be unique within a symbol's dataset across all part files.

---

## Canonical Writer

**All code writing to S3 MUST use:**

```python
from groai_fi_datastore_shared.Binance.cli.s3_utils import write_parquet_to_s3

write_parquet_to_s3(df, s3_path)  # df must have date as index
```

Direct `DuckDB COPY` is **only** permitted for read/merge operations (SELECT path),
never for the final write step.

---

## Python Validation Checklist

Run this after any write to verify spec compliance:

```python
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs, os

fs = s3fs.S3FileSystem(
    endpoint_url=os.environ["S3_ENDPOINT_URL"],
    key=os.environ["S3_ACCESS_KEY_ID"],
    secret=os.environ["S3_SECRET_ACCESS_KEY"],
)

path = "stashed-bento-3z1jiwv2yj7/prices_v3.parquet/exchange=Binance/symbol=BTCUSDT/part.00000.parquet"
schema = pq.read_schema(path, filesystem=fs)

# Rule 1: date is the index (in pandas metadata), has UTC timezone
assert "date" in schema.names
assert pa.types.is_timestamp(schema.field("date").type)
assert schema.field("date").type.tz == "UTC", f"Got tz={schema.field('date').type.tz}"
index_cols = schema.pandas_metadata.get("index_columns", [])
assert "date" in index_cols, "date must be the pandas index"

# Rule 2: yymm is dictionary-encoded
assert pa.types.is_dictionary(schema.field("yymm").type), \
    f"yymm must be dict-encoded, got {schema.field('yymm').type}"

# Rule 3: exchange and symbol NOT in file
assert "exchange" not in schema.names, "exchange must not be a file column"
assert "symbol" not in schema.names,   "symbol must not be a file column"

print("✅ Schema complies with price_parquet_v3 spec")
```

---

## Code References

| Constant / Function | File | Purpose |
|---|---|---|
| `schema.price_parquet_file_columns` | `schema.py` | Canonical list of file-level data columns |
| `schema.price_parquet_hive_columns` | `schema.py` | Partition columns (path only) |
| `write_parquet_to_s3(df, path)` | `cli/s3_utils.py` | Compliant S3 write (enforces all rules) |
| `_shape_df(df, symbol, exchange)` | `cli/download_price_binance_s3.py` | Shapes raw klines to spec |

---

*Last updated: 2026-04-14 · Applies to bucket: `stashed-bento-3z1jiwv2yj7`*
