"""
Binance CLI Entry Scripts

This package contains installed entry-point scripts for the Binance module.
All scripts are self-contained and use absolute package imports.

Entry points (registered in pyproject.toml):
    binance-download-price    → download_price_binance:run
    binance-merge-parquet     → merge_parquet_prices:run
    binance-auto-update       → auto_update_prices:main

S3 entry points:
    binance-download-price-s3 → download_price_binance_s3:run
    binance-merge-parquet-s3  → merge_parquet_prices_s3:run
    binance-auto-update-s3    → auto_update_prices_s3:main
    binance-list-symbols-s3   → list_symbols_s3:run
    binance-remove-symbol-s3  → remove_symbol_s3:run
"""

__all__ = [
    # Local scripts (original)
    'download_price_binance',
    'merge_parquet_prices',
    'auto_update_prices',
    'shared',
    'list_symbols',
    'remove_symbol',
    # S3 scripts
    's3_utils',
    'download_price_binance_s3',
    'merge_parquet_prices_s3',
    'auto_update_prices_s3',
    'list_symbols_s3',
    'remove_symbol_s3',
]
