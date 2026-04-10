"""
Binance CLI Entry Scripts

This package contains installed entry-point scripts for the Binance module.
All scripts are self-contained and use absolute package imports.

Entry points (registered in pyproject.toml):
    binance-download-price   → download_price_binance:run
    binance-merge-parquet    → merge_parquet_prices:run
    binance-auto-update      → auto_update_prices:main
"""

__all__ = [
    'download_price_binance',
    'merge_parquet_prices',
    'auto_update_prices',
    'shared',
]
