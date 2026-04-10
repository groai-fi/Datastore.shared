# groai-fi-datastore-shared

[![PyPI version](https://img.shields.io/pypi/v/groai-fi-datastore-shared.svg)](https://pypi.org/project/groai-fi-datastore-shared/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-yellow.svg)](LICENSE)
[![CI](https://github.com/groai-fi/datastore.shared/actions/workflows/publish.yml/badge.svg)](https://github.com/groai-fi/datastore.shared/actions)

Shared datastore utilities for **GroAI.fi** — Binance market data downloading, partitioned Parquet storage, order execution, and backtesting infrastructure.

---

## Features

- **Market Data Downloading** — Incremental Binance OHLCV download with automatic catch-up from where you left off
- **Parquet Storage** — Hive-partitioned Parquet files (`exchange=X/symbol=Y/part.N.parquet`) powered by Dask
- **File Locking** — Safe concurrent writes via `.write.lock` file with stale-lock detection
- **Order Execution** — `BinanceOrder`, `BinanceClient` with spot and margin support
- **Backtesting** — `BacktestOrder`, `BacktestOrderData` for strategy simulation
- **CLI Tools** — Three ready-to-use command-line scripts for data management

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
| `BINANCE_API_KEY_TEST` | No | Testnet API key |
| `BINANCE_API_SECRET_TEST` | No | Testnet API secret |
| `SEND_MAIL_RECEIVER` | No | Email address for trade alerts |

Create a `.env` file in your project root and load it with `python-dotenv` or export variables in your shell.

---

## CLI Usage

### Download price data

```bash
binance-download-price \
  --symbol BTCUSDT \
  --tframe 1m \
  --path /absolute/path/to/prices_v3.parquet \
  --start_date 2024/01/01
```

### Merge / compact parquet files

```bash
binance-merge-parquet \
  --exchange Binance \
  --symbol BTCUSDT \
  --path /absolute/path/to/prices_v3.parquet \
  --interval_base 1m
```

### Auto-update all symbols

```bash
binance-auto-update \
  --exchange-dir /absolute/path/to/prices_v3.parquet/exchange=Binance \
  --price-root /absolute/path/to/prices_v3.parquet \
  --tframe 1m
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
