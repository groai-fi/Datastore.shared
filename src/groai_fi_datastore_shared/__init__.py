"""
groai-fi-datastore-shared
=========================
Shared datastore utilities for GroAI.fi.

Provides Binance market data downloading, parquet storage management,
order execution, and backtesting infrastructure.

Subpackages
-----------
Binance
    Full Binance API integration — market data, orders, backtesting, and CLI tools.

Usage
-----
    from groai_fi_datastore_shared.Binance import BinanceMarketDataDownloader
    from groai_fi_datastore_shared.Binance import BinanceClient
"""

__version__ = "0.1.0"
__author__ = "Hamilton"
__email__ = "hamilton@aiart.io"

# Expose the Binance sub-package at the top level for convenience
from groai_fi_datastore_shared import Binance

__all__ = [
    "__version__",
    "__author__",
    "__email__",
    "Binance",
]
