"""
Auto update prices for all symbols

Usage:
    python auto_update_prices.py
"""
import os
import sys
import pandas as pd
import datetime as dt
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
SCRIPT_DIR = Path(__file__).resolve().parent
BINANCE_DIR = SCRIPT_DIR.parent
PROJECT_ROOT = BINANCE_DIR.parent.parent
sys.path.insert(0, str(BINANCE_DIR.parent))

# Import Binance modules
from Binance import BinanceMarketDataDownloader, helper
from Binance.utils import setup_logger, get_project_root, readable_error

# Configuration
APP_DATA_DIR = PROJECT_ROOT / "appData"
PRICES_DIR = APP_DATA_DIR / "trainData_crypto" / "prices_v3.parquet"
EXCHANGE_DIR = PRICES_DIR / "exchange=Binance"


def get_last_date(symbol_path):
    """Get the last date from parquet files"""
    try:
        # Optimization: Find the last partition file and read only that
        files = list(symbol_path.glob("part.*.parquet"))
        if not files:
            # Fallback for non-partitioned or standard read if no parts found
            if not any(symbol_path.iterdir()):
                return None
            df = pd.read_parquet(symbol_path, engine='pyarrow')
        else:
            # Sort by partition number (part.N.parquet)
            try:
                files.sort(key=lambda x: int(x.name.split('.')[1]))
                last_file = files[-1]
                # Read only the last file
                df = pd.read_parquet(last_file, engine='pyarrow')
            except Exception as e:
                print(f"Warning: Error identifying last partition in {symbol_path}: {e}. Reading full dir.")
                df = pd.read_parquet(symbol_path, engine='pyarrow')

        if df.empty:
            return None
        
        # Assume index is date or 'date' column exists 
        if 'date' in df.columns:
            last_date = df['date'].max()
        elif isinstance(df.index, pd.DatetimeIndex):
            last_date = df.index.max()
        else:
            print(f"Warning: Could not determine date column for {symbol_path}")
            return None
            
        return last_date
    except Exception as e:
        print(f"Error reading parquet {symbol_path}: {e}")
        return None


def download_symbol(symbol, start_date, logger):
    """Download price data for a symbol"""
    try:
        print(f"  Downloading from {start_date.strftime('%Y/%m/%d')}...")
        
        result = BinanceMarketDataDownloader.catchup_price_binance(
            symbol=symbol,
            kline_tframe="1m",
            default_download_start_date=start_date,
            price_root_dir="appData/trainData_crypto/prices_v3.parquet",
            logger=logger
        )
        
        return result is not None
    except Exception as e:
        err = readable_error(e, __file__)
        logger.error(f"Download failed for {symbol}: {err}")
        print(f"  Error: {err}")
        return False


def merge_symbol(symbol, logger):
    """Merge and compact price data for a symbol"""
    try:
        print(f"  Merging and compacting...")
        
        price_dir = f"{get_project_root()}/appData/trainData_crypto/prices_v3.parquet"
        price_dir_full = f"{price_dir}/exchange=Binance/symbol={symbol}"
        
        # Backup
        now_str = dt.datetime.now().strftime('%Y%m%dT%H%M%S')
        backup_dir = f"{price_dir_full}_{now_str}"
        
        # Load prices
        price_dd = helper.load_base_price(
            exchange="Binance",
            symbol=symbol,
            price_data_path=price_dir,
            interval_base="1m",
            cols=None,
            index=False
        )
        
        if price_dd is None:
            logger.error(f"Failed to load price data for {symbol}")
            return False
        
        # Backup
        from shared import copy_dir
        try:
            copy_dir(price_dir_full, backup_dir, logger)
            logger.info(f'Backup to {backup_dir}')
        except Exception as e:
            err = readable_error(e, __file__)
            logger.warning(f'Backup failed: {err}')
        
        # Compute to pandas
        price_pd = price_dd.compute()
        
        # Reset index if needed
        if price_pd.index.name in [None, '__null_dask_index__']:
            price_pd = price_pd.reset_index(drop=True)
        
        # Ensure required columns
        if 'exchange' not in price_pd.columns:
            price_pd['exchange'] = "Binance"
        if 'symbol' not in price_pd.columns:
            price_pd['symbol'] = symbol
        
        # Set date as index
        if 'date' in price_pd.columns and price_pd.index.name != 'date':
            price_pd.set_index('date', inplace=True)
        
        # Save merged
        helper.save_price_parquet(
            price_pd,
            price_dir_full,
            append=False,
            overwrite=True,
            n_partitions=10
        )
        
        logger.info(f'Successfully saved merged data to {price_dir_full}')
        return True
        
    except Exception as e:
        err = readable_error(e, __file__)
        logger.error(f"Merge failed for {symbol}: {err}")
        print(f"  Error: {err}")
        return False


def get_earliest_date(symbol, logger):
    """Get the earliest available date for a symbol from Binance API"""
    try:
        from binance.client import Client
        import os
        
        # Initialize client
        client = Client(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"), testnet=False)
        
        # Get first candle
        first_candle = client.get_klines(symbol=symbol, interval='1m', startTime=0, limit=1)
        
        if first_candle and len(first_candle) > 0:
            # First element is open time in milliseconds
            first_timestamp_ms = first_candle[0][0]
            first_date = datetime.fromtimestamp(first_timestamp_ms / 1000)
            logger.info(f"Earliest available date for {symbol}: {first_date}")
            return first_date
        else:
            logger.warning(f"No candles found for {symbol}, using default date")
            return datetime(2013, 1, 1)
            
    except Exception as e:
        logger.warning(f"Failed to fetch earliest timestamp for {symbol}: {e}. Using default date.")
        return datetime(2013, 1, 1)


def main():
    """Main entry point"""
    if not EXCHANGE_DIR.exists():
        print(f"Error: Exchange directory not found at {EXCHANGE_DIR}")
        return

    # 1. Discover Symbols
    discovered_symbols = []
    if EXCHANGE_DIR.exists():
        for symbol_dir in EXCHANGE_DIR.iterdir():
            if symbol_dir.is_dir() and symbol_dir.name.startswith("symbol="):
                symbol = symbol_dir.name.replace("symbol=", "")
                discovered_symbols.append(symbol)

    if not discovered_symbols:
        print("No symbols found in exchange directory")
        return

    print(f"Found {len(discovered_symbols)} symbols: {', '.join(discovered_symbols[:10])}...")
    if len(discovered_symbols) > 10:
        print(f"  ... and {len(discovered_symbols) - 10} more")

    # 2. Process each symbol
    success_count = 0
    fail_count = 0
    
    for symbol in discovered_symbols:
        print(f"\n{'='*60}")
        print(f"Processing {symbol}")
        print(f"{'='*60}")

        # Setup logger for this symbol
        logger = setup_logger('auto_update_prices.log', symbol)
        
        symbol_path = EXCHANGE_DIR / f"symbol={symbol}"
        
        # Get last date
        last_date = get_last_date(symbol_path)
        
        if last_date is None:
            print(f"Could not determine last date for {symbol}, fetching earliest from Binance API...")
            start_date = get_earliest_date(symbol, logger)
            print(f"Starting from earliest available: {start_date.strftime('%Y/%m/%d')}")
        else:
            # Add 1 day to last date
            next_date = pd.Timestamp(last_date) + timedelta(days=1)
            start_date = next_date.to_pydatetime()
            print(f"Last date: {last_date}, starting from: {start_date.strftime('%Y/%m/%d')}")

        # 3. Download
        if not download_symbol(symbol, start_date, logger):
            print(f"✗ Download failed for {symbol}, skipping merge")
            fail_count += 1
            continue

        # 4. Merge if too many parquet files
        num_parquets = len(list(symbol_path.glob("part.*.parquet")))
        if num_parquets > 50:
            if not merge_symbol(symbol, logger):
                print(f"✗ Merge failed for {symbol}")
                fail_count += 1
                continue
        else:
            print(f"Skipping merge, only {num_parquets} parquet files (threshold: 50)")

        print(f"✓ Successfully updated {symbol}")
        success_count += 1

    print(f"\n{'='*60}")
    print(f"Auto update completed!")
    print(f"  Success: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Total: {len(discovered_symbols)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
