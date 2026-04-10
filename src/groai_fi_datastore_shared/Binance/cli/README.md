# Binance CLI Entry Scripts

This directory contains standalone entry scripts for the Binance module. All scripts are self-contained and can be called directly from the command line.

---

## Script Relationships

Understanding how the scripts relate to each other is important when choosing which one to run.

```
download_data_from_binance_1minute()   ← stateless Binance API fetcher
        │                                (BinanceMarketDataDownloader.py)
        │                                Returns a plain pandas DataFrame.
        │                                Has NO knowledge of local paths or S3.
        │
        ├─▶ download_price_binance.py      saves locally via Dask / pyarrow
        └─▶ download_price_binance_s3.py   saves to S3 via DuckDB COPY

auto_update_prices.py      discovers symbols via local filesystem → orchestrates local scripts
auto_update_prices_s3.py   discovers symbols via S3 boto3 listing  → orchestrates S3 scripts
```

**Key insight:** If you change the download logic in `BinanceMarketDataDownloader.py`
(e.g. retry strategy, batch step size), both local and S3 download scripts benefit
automatically because they share the same API fetcher. Only the *save* step differs.

---

## S3 Scripts

The S3 scripts upload data directly to an S3-compatible object store using DuckDB's
`httpfs` extension. No local volume or persistent disk is required.

### Required Environment Variables

```bash
export S3_ENDPOINT_URL=https://t3.storageapi.dev
export S3_BUCKET_NAME=stashed-bento-3z1jiwv2yj7
export S3_ACCESS_KEY_ID=<your-key>
export S3_SECRET_ACCESS_KEY=<your-secret>

# Still needed for Binance API access:
export BINANCE_API_KEY=<your-key>
export BINANCE_API_SECRET=<your-secret>
```

### S3 Data Layout

```
s3://<bucket>/prices_v3.parquet/
└── exchange=Binance/
    └── symbol=BTCUSDT/
        ├── part.00000.parquet    ← single merged file (post-merge)
        ├── part.1744000000.parquet  ← appended since last merge
        └── part.1744001000.parquet
```

The glob `part.*.parquet` always includes `part.00000.parquet` (00000 matches *),
so `get_max_date_s3` and future appends work seamlessly across merged and new parts.

### Symbol Lifecycle: Add / Remove / List / Update

```bash
# ── INSPECT ──────────────────────────────────────────────
# See all tracked symbols with last date, part count, status
binance-list-symbols-s3

# ── ADD A NEW TOKEN ───────────────────────────────────────
# One-time bootstrap — auto_update-s3 discovers it automatically after this
binance-download-price-s3 --symbol SOLUSDT --start-date 2021/01/01

# ── REMOVE A TOKEN ────────────────────────────────────────
# Interactive (prompts you to type the symbol name to confirm)
binance-remove-symbol-s3 --symbol SOLUSDT

# Non-interactive (for scripts / cron jobs)
binance-remove-symbol-s3 --symbol SOLUSDT --yes

# ── UPDATE ALL TRACKED TOKENS ─────────────────────────────
# Discovers all symbols on S3, downloads gaps, merges when needed
binance-auto-update-s3

# ── UPDATE A SINGLE TOKEN ─────────────────────────────────
binance-download-price-s3 --symbol BTCUSDT
binance-merge-parquet-s3 --symbol BTCUSDT   # optional, only if compaction needed

# ── DEBUG ─────────────────────────────────────────────────
# Merge without deleting old parts (inspect before committing)
binance-merge-parquet-s3 --symbol BTCUSDT --no-delete-parts

# Auto-update without triggering merge (high threshold)
binance-auto-update-s3 --merge-threshold 9999
```

### Complete S3 Command Reference

| Command | Description |
|---|---|
| `binance-download-price-s3` | Download/append data for one symbol → S3 |
| `binance-merge-parquet-s3` | Merge all parts → `part.00000.parquet`, delete old parts |
| `binance-auto-update-s3` | Auto-discover all symbols, download gaps, merge if needed |
| `binance-list-symbols-s3` | Inspect tracked symbols: last date, part count, status |
| `binance-remove-symbol-s3` | Permanently delete all S3 data for a symbol |

### Merge Lifecycle

```
Every N minutes (cron / scheduled job):
    binance-download-price-s3 → part.<ts>.parquet appended to S3

When parts > 50 (auto_update-s3 threshold):
    1. Read all part.*.parquet via DuckDB glob
    2. Deduplicate by date, sort ascending
    3. Write part.00000.parquet to S3
    4. Validate: merged row count == pre-merge row count
    5. Delete all old part.<ts>.parquet (keep part.00000.parquet)

Next download cycle:
    part.<ts>.parquet sits alongside part.00000.parquet
    → both included in glob → get_max_date_s3 works correctly ✅
```

---

## Available Scripts (Local)


### 1. download_price_binance.py

Download historical price data from Binance.

**Usage:**
```bash
python download_price_binance.py \
    --exchange Binance \
    --symbol BTCUSDT \
    --tframe 1m \
    --path appData/trainData_crypto/prices_v3.parquet \
    --start_date 2024/01/01
```

**Arguments:**
- `--exchange`: Exchange name (default: Binance)
- `--symbol`: Trading pair symbol (e.g., BTCUSDT)
- `--tframe`: Timeframe (e.g., 1m, 5m, 1h)
- `--path`: Path to store price data
- `--start_date`: Start date in YYYY/MM/DD format

### 2. merge_parquet_prices.py

Merge and compact parquet price files into fewer partitions.

**Usage:**
```bash
python merge_parquet_prices.py \
    --exchange Binance \
    --symbol BTCUSDT \
    --path appData/trainData_crypto/prices_v3.parquet \
    --interval_base 1m
```

**Arguments:**
- `--exchange`: Exchange name
- `--symbol`: Trading pair symbol
- `--path`: Path to price data
- `--interval_base`: Base interval (e.g., 1m)

**Note:** This script creates a backup before merging.

### 3. auto_update_prices.py

Automatically update prices for all symbols in the data directory.

**Usage:**
```bash
python auto_update_prices.py
```

**Features:**
- Automatically discovers all symbols in `appData/trainData_crypto/prices_v3.parquet/exchange=Binance/`
- Determines the last date for each symbol
- Downloads new data from the last date + 1 day
- Merges the data into 10 partitions

**Configuration:**
- Uses `.venv/bin/python` if available
- Falls back to system Python if venv not found

### 4. test_download_price_binance.py

Unit tests for download and merge functionality.

**Usage:**
```bash
python test_download_price_binance.py
```

**Tests:**
1. Fresh download from empty directory
2. Merge parquet files
3. Gap filling (deletes a middle partition and re-downloads)

**Configuration:**
- Test symbol: `BCHUSDT`
- Test start date: `2026/01/19`

### 5. shared.py

Shared utility functions used by the entry scripts.

**Functions:**
- `copy_dir(source_dir, destination_dir, logger)`: Copy directory with logging
- `del_dir(destination_dir, logger)`: Delete directory with logging

## Dependencies

All scripts depend on the parent Binance module:
- `Binance.BinanceMarketDataDownloader`
- `Binance.helper`
- `Binance.utils`
- `Binance.schema`

## Running from Project Root

All scripts are designed to be run from the project root directory:

```bash
cd /Users/hamiltonwang/MyCode/AIHedge/groai.01
.venv/bin/python 3rd_party/Binance/entry/download_price_binance.py --exchange Binance --symbol BTCUSDT --tframe 1m --path appData/trainData_crypto/prices_v3.parquet --start_date 2024/01/01
```

## Environment Setup

The scripts automatically:
1. Add the parent directory to `sys.path` for imports
2. Use `.venv/bin/python` if available
3. Create log files in `logs/` directory
4. Handle relative paths from project root

## Logging

All scripts create log files in the `logs/` directory:
- `script_download_price_binance.log`
- `script_merge_parquet_prices.log`

Logs include:
- Timestamp
- Log level
- Script name
- Line number
- Thread ID
- Message

## Error Handling

All scripts:
- Use `readable_error()` for detailed error messages
- Exit with status code 1 on error
- Log errors to both console and log file

## Data Structure

Expected data directory structure:
```
appData/trainData_crypto/prices_v3.parquet/
└── exchange=Binance/
    └── symbol=BTCUSDT/
        ├── part.0.parquet
        ├── part.1.parquet
        └── ...
```

## Notes

- All scripts are standalone and don't depend on external `app.*`, `api.*`, or `config.*` modules
- The Binance module must be properly modularized (see parent README_MODULARIZATION.md)
- Scripts use the virtual environment Python if available
