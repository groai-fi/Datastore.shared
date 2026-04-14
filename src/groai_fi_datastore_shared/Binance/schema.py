"""Schema definitions for Binance data"""

# Price parquet schema
price_parquet = {
    'date': 'datetime64[ns]',  # index
    'yymm': 'string[pyarrow]',
    'exchange': 'string[pyarrow]',
    'symbol': 'string[pyarrow]',
    'open': 'float64',
    'high': 'float64',
    'low': 'float64',
    'close': 'float64',
    'volume': 'float64'
}

price_header_parquet = ['date', 'yymm', 'exchange', 'symbol', 'open', 'high', 'low', 'close', 'volume']

# price_parquet_v3 split — see PRICE_PARQUET_V3_SPEC.md for the full contract
# Columns written INSIDE the Parquet file (date is the index, not a column)
price_parquet_file_columns = ['yymm', 'open', 'high', 'low', 'close', 'volume']
# Columns encoded in the Hive S3 path only — MUST NOT appear in file data
price_parquet_hive_columns = ['exchange', 'symbol']

# Required for BinanceMarketDataDownloader.py line 426
price_columns = ['date', 'open', 'high', 'low', 'close', 'volume']

# Required for BinanceMarketDataDownloader.py line 213 (must start with 'date')
# Raw columns from Binance API klines
binance_columns = [
    'date',                         # Open time
    'open',                         # Open
    'high',                         # High
    'low',                          # Low
    'close',                        # Close
    'volume',                       # Volume
    'close_time',                   # Close time
    'quote_asset_volume',           # Quote asset volume
    'number_of_trades',             # Number of trades
    'taker_buy_base_asset_volume',  # Taker buy base asset volume
    'taker_buy_quote_asset_volume', # Taker buy quote asset volume
    'ignore'                        # Ignore
]

# Types for raw binance columns
binance_columns_type = {
    'date': 'int64',
    'open': 'float64',
    'high': 'float64',
    'low': 'float64',
    'close': 'float64',
    'volume': 'float64',
    'close_time': 'int64',
    'quote_asset_volume': 'float64',
    'number_of_trades': 'int64',
    'taker_buy_base_asset_volume': 'float64',
    'taker_buy_quote_asset_volume': 'float64',
    'ignore': 'float64'
}

# Alias used in some parts of the code
binance_product_columns = binance_columns_type

# Date formats
datefmt = '%Y-%m-%d'
datetimefmt = '%Y-%m-%d %H:%M:%S'
datefmtnano = '%Y-%m-%dT%H:%M:%S.%f'

# Last tick columns (for BinanceOrder.py)
last_tick_columns = ['date', 'last_price', 'ask_price', 'bid_price']
last_tick_columns_type = {
    'date': 'datetime64[ns]',
    'last_price': 'float64',
    'ask_price': 'float64',
    'bid_price': 'float64'
}
