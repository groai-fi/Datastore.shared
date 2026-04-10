# Binance Module - Standalone Package

## Overview

The `3rd_party/Binance` module is now a **fully standalone package** with no external dependencies on `app.*`, `api.*`, `config.*`, `envs.*`, or `groai.*` packages.

## What Changed

### New Internal Modules Created

1. **`config.py`** - Configuration management
   - `BinanceConfig` dataclass for all configuration
   - Backward compatible module-level variables (`parquet_engine`, `tw_tz`, etc.)
   - `get_default_config()` and `set_default_config()` helpers

2. **`schema.py`** - Data schemas
   - All Binance column definitions
   - Price parquet schema
   - Date format constants
   - Replaces `groai.common.legacy_schema`

3. **`enums.py`** - Enumerations
   - `TradeAction`, `ErrorCodes`, `AppEnv`, `OrderStatus`
   - Replaces `envs.TradeEnum` and `api.api_share`

4. **`utils.py`** - Utility functions
   - Decimal handling (`d`, `d_round`, `d_abs`, etc.)
   - Error formatting (`readable_error`)
   - Helper functions
   - Replaces `app.utils`

5. **`email_utils.py`** - Email functionality
   - `EmailSender` class with callback pattern
   - `send_mail` function for backward compatibility
   - Replaces `api.SendMail.SendMail`

6. **`__init__.py`** - Package interface
   - Clean public API
   - Graceful import error handling
   - Exports all commonly used classes and functions

### Files Modified

All existing files were updated to use internal modules:
- `BinanceMarketDataDownloader.py` - Uses internal `schema` and `config`
- `helper.py` - Uses internal `schema` and `config`
- `BinanceOrder.py` - Uses internal `config`, `enums`, `utils`, `email_utils`, `schema`
- `BinanceClient.py` - Uses internal `config`
- `BacktestOrder.py` - Uses internal `enums` and `utils`
- `BacktestOrderData.py` - Uses internal `enums` and `utils`
- `BinanceOrderData.py` - Uses internal `enums` and `utils`
- `OrderBook.py` - Uses internal `config` and `utils`
- `BinanceMarketData.py` - Uses internal modules
- `DataReadWrite.py` - Uses internal `utils`

## Usage

### Basic Import

```python
import sys
sys.path.insert(0, '3rd_party')

from Binance import (
    BinanceConfig,
    BinanceMarketDataDownloader,
    schema,
    d, d_round,
    save_price_parquet,
    FileLock
)
```

### Configuration

#### Option 1: Environment Variables (Recommended)

```python
from Binance import BinanceConfig

# Automatically loads from environment variables:
# - BINANCE_API_KEY
# - BINANCE_API_SECRET
# - BINANCE_API_KEY_TEST
# - BINANCE_API_SECRET_TEST
# - SEND_MAIL_RECEIVER
config = BinanceConfig.from_env()
```

#### Option 2: Explicit Configuration

```python
from Binance import BinanceConfig

config = BinanceConfig(
    api_key="your_api_key",
    api_secret="your_api_secret",
    api_key_test="test_key",
    api_secret_test="test_secret",
    parquet_engine="pyarrow",
    recv_window=60000
)
```

#### Option 3: Set Global Default

```python
from Binance import BinanceConfig, set_default_config

config = BinanceConfig(api_key="...", api_secret="...")
set_default_config(config)

# Now all modules will use this config by default
```

### Using the Downloader

```python
from Binance import BinanceMarketDataDownloader

# The module provides functions like:
# - get_asset()
# - catchup_price_binance(...)
# - download_price_binance(...)

# Example:
BinanceMarketDataDownloader.catchup_price_binance(
    symbol="BTCUSDT",
    interval="1m",
    path="data/prices",
    default_download_start_date="2024/01/01"
)
```

### Using Trading Classes

```python
from Binance import BinanceOrder, BinanceConfig

config = BinanceConfig.from_env()
order = BinanceOrder(
    exch_mode="SpotAPI",  # or "SpotTest"
    _logger=your_logger,
    config=config
)
```

### Email Notifications (Optional)

```python
from Binance import EmailSender

def my_email_handler(receivers, subject, content):
    # Your custom email implementation
    print(f"Sending to {receivers}: {subject}")

email_sender = EmailSender(callback=my_email_handler)
email_sender.send_mail(
    ["user@example.com"],
    "Alert",
    "Price download complete"
)
```

## Migration Guide

### For Existing Code

If you have existing code using the old imports:

**Before:**
```python
from groai.common.config import parquet_engine, tw_tz
import groai.common.legacy_schema as schema
from app.utils import d, d_round
from config import app_config
```

**After:**
```python
from Binance.config import parquet_engine, tw_tz
from Binance import schema
from Binance.utils import d, d_round
from Binance import BinanceConfig

config = BinanceConfig.from_env()
```

### For Download Scripts

Update `script/download_price/download_price_binance.py`:

```python
import sys
sys.path.insert(0, '3rd_party')

from Binance import BinanceMarketDataDownloader, BinanceConfig

# Set up config
config = BinanceConfig.from_env()

# Use the downloader
BinanceMarketDataDownloader.catchup_price_binance(...)
```

## Benefits

✅ **No External Dependencies** - Completely self-contained  
✅ **Reusable** - Can be used by multiple projects  
✅ **Configurable** - Flexible configuration system  
✅ **Backward Compatible** - Easy migration path  
✅ **Well Organized** - Clear separation of concerns  
✅ **Type Safe** - Uses dataclasses and type hints  

## Testing

Verify the module works:

```bash
cd /Users/hamiltonwang/MyCode/AIHedge/groai.01
.venv/bin/python -c "
import sys
sys.path.insert(0, '3rd_party')
from Binance import BinanceConfig, schema
print('✓ Import successful')
print(f'✓ Schema columns: {len(schema.binance_columns)}')
"
```

## Notes

- The module still requires `python-binance`, `pandas`, `dask`, `pyarrow`, and `numpy`
- All 14 original files are preserved with full functionality
- Configuration is now explicit and testable
- Email functionality is optional (no-op if no callback provided)

## Future Improvements

- Add unit tests for all modules
- Create `setup.py` for pip installation
- Add type stubs for better IDE support
- Document all public APIs with docstrings
- Add examples directory with common use cases
