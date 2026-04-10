"""
Binance Trading and Data Module

A standalone module for Binance API integration, including:
- Market data downloading
- Order execution (spot and margin)
- Backtesting infrastructure
- Data I/O utilities

This module is self-contained and does not depend on external packages
like app.*, api.*, config.*, or envs.*.
"""

from .config import BinanceConfig, parquet_engine, tw_tz, us_tz
from .enums import TradeAction, ErrorCodes, AppEnv, OrderStatus
from . import schema
from .utils import (
    d, d_round, d_round_fee, d_abs, d_negate, d_is_close,
    readable_error, get_project_root, normalize_fraction,
    least_significant_digit_power, pretty_dict, convert_to_min,
    save_data
)
from .email_utils import EmailSender, send_mail

# Import main modules
from . import BinanceMarketDataDownloader
from .helper import save_price_parquet, FileLock

# Import trading classes (these have more dependencies)
try:
    from .BinanceClient import BinanceClient
    from .BinanceOrder import BinanceOrder
    from .BacktestOrder import BacktestOrder
    from .BacktestOrderData import BacktestOrderData
    from .BinanceOrderData import BinanceOrderData
    from .BaseBacktestOrder import BaseBacktestOrder
except ImportError as e:
    # Some classes may not be importable if dependencies are missing
    import warnings
    warnings.warn(f"Some Binance classes could not be imported: {e}")

__version__ = "1.0.0"

__all__ = [
    # Configuration
    "BinanceConfig",
    "parquet_engine",
    "tw_tz",
    "us_tz",
    
    # Schema
    "schema",
    
    # Enums
    "TradeAction",
    "ErrorCodes",
    "AppEnv",
    "OrderStatus",
    
    # Utilities
    "d",
    "d_round",
    "d_round_fee",
    "d_abs",
    "d_negate",
    "d_is_close",
    "readable_error",
    "get_project_root",
    "normalize_fraction",
    "least_significant_digit_power",
    "pretty_dict",
    "convert_to_min",
    "save_data",
    
    # Email
    "EmailSender",
    "send_mail",
    
    # Market Data
    "BinanceMarketDataDownloader",
    "save_price_parquet",
    "FileLock",
    
    # Trading (if available)
    "BinanceClient",
    "BinanceOrder",
    "BacktestOrder",
    "BacktestOrderData",
    "BinanceOrderData",
    "BaseBacktestOrder",
]
