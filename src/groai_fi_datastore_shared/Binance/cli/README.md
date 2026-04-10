# Binance Entry Scripts

This directory contains standalone entry scripts for the Binance module. All scripts are self-contained and can be called directly from the command line.

## Available Scripts

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
