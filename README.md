# groai-fi-datastore-shared

[![PyPI version](https://img.shields.io/pypi/v/groai-fi-datastore-shared.svg)](https://pypi.org/project/groai-fi-datastore-shared/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-yellow.svg)](LICENSE)
[![CI](https://github.com/groai-fi/datastore.shared/actions/workflows/publish.yml/badge.svg)](https://github.com/groai-fi/datastore.shared/actions)

Shared datastore utilities for **GroAI.fi** — Binance market data downloading, partitioned Parquet storage, order execution, and backtesting infrastructure.

---

## Features

- **Market Data Downloading** — Incremental Binance OHLCV download with automatic catch-up from where you left off
- **Parquet Storage** — Hive-partitioned Parquet files (`exchange=X/symbol=Y/part.N.parquet`) powered by Dask (local) and DuckDB (S3)
- **File Locking** — Safe concurrent writes via `.write.lock` file with stale-lock detection (Local filesystem only)
- **Order Execution** — `BinanceOrder`, `BinanceClient` with spot and margin support
- **Backtesting** — `BacktestOrder`, `BacktestOrderData` for strategy simulation
- **CLI Suite** — 10 distinct CLI commands enabling both isolated local-filesystem and cloud-native S3 workflows

---

## Installation

```bash
# Using pip
pip install groai-fi-datastore-shared

# Using uv (recommended)
uv add groai-fi-datastore-shared
```

---

## Environment Variables

The following environment variables are used across modules:

| Variable | Required | Description |
|---|---|---|
| `BINANCE_API_KEY` | Yes (trading/download) | Your Binance API key |
| `BINANCE_API_SECRET` | Yes (trading/download) | Your Binance API secret |
| `S3_ENDPOINT_URL` | Yes (if S3) | Your object storage endpoint (e.g. `https://t3.storageapi.dev`) |
| `S3_BUCKET_NAME` | Yes (if S3) | The target bucket name |
| `S3_ACCESS_KEY_ID` | Yes (if S3) | S3 access key ID |
| `S3_SECRET_ACCESS_KEY` | Yes (if S3) | S3 standard secret access key |
| `BINANCE_API_KEY_TEST` | No | Testnet API key |
| `BINANCE_API_SECRET_TEST` | No | Testnet API secret |
| `SEND_MAIL_RECEIVER` | No | Email address for trade alerts |

Create a `.env` file in your project root and load it with `python-dotenv` or export variables in your shell.

---

## CLI Usage

The package exposes identical pipelines for **Local Filesystems** and **S3 Storage**.

### Local Storage Commands
Run data engineering workflows directly onto a mounted drive via Dask and PyArrow files:
```bash
# 1. Download
binance-download-price --symbol BTCUSDT --tframe 1m --path /path/to/prices_v3.parquet
# 2. Merge shards
binance-merge-parquet --exchange Binance --symbol BTCUSDT --path /path/to/prices_v3.parquet --interval_base 1m
# 3. Auto-catchup all tracked symbols
binance-auto-update --exchange Binance --path /path/to/prices_v3.parquet --tframe 1m
# 4. View tracked symbols
binance-list-symbols --path /path/to/prices_v3.parquet
# 5. Clean / remove
binance-remove-symbol --symbol BTCUSDT --path /path/to/prices_v3.parquet --yes
```

### S3 Storage Commands (Native)
Run structurally identical commands operating directly on object storage leveraging DuckDB HTTPFS. Local storage volumes are not required.
```bash
# S3 1. Download
binance-download-price-s3 --symbol BTCUSDT --tframe 1m --bucket my-bucket
# S3 2. Merge shards (Memory optimized via DuckDB)
binance-merge-parquet-s3 --exchange Binance --symbol BTCUSDT --bucket my-bucket
# S3 3. Auto-catchup all tracking
binance-auto-update-s3 --exchange Binance --bucket my-bucket --tframe 1m
# S3 4. View S3 inventory
binance-list-symbols-s3 --bucket my-bucket
# S3 5. Clean / remove
binance-remove-symbol-s3 --symbol BTCUSDT --bucket my-bucket --yes
```

---

## Python API

```python
# Market data downloading
from groai_fi_datastore_shared.Binance import BinanceMarketDataDownloader
from datetime import datetime

logger = ...  # your logger

BinanceMarketDataDownloader.catchup_price_binance(
    symbol="BTCUSDT",
    kline_tframe="1m",
    default_download_start_date=datetime(2024, 1, 1),
    price_root_dir="/data/prices_v3.parquet",
    logger=logger,
)

# Reading config from environment
from groai_fi_datastore_shared.Binance.config import BinanceConfig
config = BinanceConfig.from_env()

# Trading client
from groai_fi_datastore_shared.Binance import BinanceClient
client = BinanceClient(config)
```

---

## Development

```bash
# Clone
git clone https://github.com/groai-fi/datastore.shared.git
cd datastore.shared

# Install with dev extras
make install-dev

# Run tests
make test

# Lint
make lint
```

---

## Publishing

```bash
# Build wheel + sdist
make build

# Publish to PyPI (requires UV_PUBLISH_TOKEN in env)
make publish

# Publish to TestPyPI first
make publish-test
```

To release a new version:
1. Bump the `version` field in `pyproject.toml`
2. Commit and push
3. Tag: `git tag v0.2.0 && git push origin v0.2.0`
4. GitHub Actions will automatically build and publish to PyPI

---

## License

[Apache License 2.0](LICENSE)

