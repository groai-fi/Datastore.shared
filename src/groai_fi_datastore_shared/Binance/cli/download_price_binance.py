"""
Download Binance price data

Usage:
    python download_price_binance.py --exchange Binance --symbol BTCUSDT --tframe 1m --path appData/trainData_crypto/prices_v3.parquet --start_date 2024/01/01
"""
import os
import sys
import argparse
from datetime import datetime as dt
from pathlib import Path

# Add parent directory to path for imports
SCRIPT_DIR = Path(__file__).resolve().parent
BINANCE_DIR = SCRIPT_DIR.parent
sys.path.insert(0, str(BINANCE_DIR.parent))

# Import from Binance module
from Binance import BinanceMarketDataDownloader
from Binance.utils import setup_logger, readable_error


def parse_arguments():
    parser = argparse.ArgumentParser(description='Download Binance price data')

    parser.add_argument('--exchange', type=str, required=False,
                        default='Binance', help='exchange (default: Binance)')

    parser.add_argument('--symbol', type=str, required=True,
                        help='symbol')

    parser.add_argument('--tframe', type=str, required=False,
                        default='1m', help='kline_tframe (default: 1m)')

    parser.add_argument('--path', type=str, required=True,
                        help='price_data_path')

    parser.add_argument('--start_date', type=str, required=True,
                        help='start date in format YYYY/MM/DD')

    parser.add_argument('--remove_old', action='store_true',
                        help='remove old data (flag)')

    cmd_args = parser.parse_args()

    return cmd_args


def run_binance(cmd_args: dict, logger):
    """Run Binance price download"""
    start_date = dt.strptime(cmd_args['start_date'], '%Y/%m/%d')

    BinanceMarketDataDownloader.catchup_price_binance(
        cmd_args['symbol'],
        cmd_args['tframe'],
        start_date,
        cmd_args['path'],
        logger
    )


def run():
    """Main entry point"""
    cmd_args = vars(parse_arguments())
    # easy read
    # cmd_args = {
    #     "exchange": "Binance",
    #     "symbol": "BTCUSDT",
    #     "tframe": "1m",
    #     "path": "appData/trainData_crypto/prices_v3.parquet",
    #     "start_date": "2013/01/01"
    # }
    if cmd_args['symbol'] in ["BTCUSDT", "ETHUSDT", "LTCUSDT"]:
        cmd_args['start_date'] = "2018/03/01"
    
    logger = setup_logger('script_download_price_binance.log', cmd_args['symbol'])

    try:
        if cmd_args['exchange'] == "Binance":
            run_binance(cmd_args, logger)
        else:
            raise Exception(f"unknown exchange {cmd_args['exchange']}")

    except Exception as e:
        err = readable_error(e, __file__)
        logger.error(err)
        sys.exit(1)


if __name__ == '__main__':
    run()
    print("Script finished. Exiting explicitly.")
    sys.exit(0)
