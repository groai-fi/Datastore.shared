"""
Download Binance price data

Usage (installed CLI):
    binance-download-price --symbol BTCUSDT --tframe 1m --path /data/prices_v3.parquet --start_date 2024/01/01

Usage (direct):
    python -m groai_fi_datastore_shared.Binance.cli.download_price_binance --symbol BTCUSDT ...
"""
import sys
import argparse
from datetime import datetime as dt

# Import from installed package
from groai_fi_datastore_shared.Binance import BinanceMarketDataDownloader
from groai_fi_datastore_shared.Binance.utils import setup_logger, readable_error


def parse_arguments():
    parser = argparse.ArgumentParser(description='Download Binance price data')

    parser.add_argument('--exchange', type=str, required=False,
                        default='Binance', help='exchange (default: Binance)')

    parser.add_argument('--symbol', type=str, required=True,
                        help='symbol (e.g. BTCUSDT)')

    parser.add_argument('--tframe', type=str, required=False,
                        default='1m', help='kline_tframe (default: 1m)')

    parser.add_argument('--path', type=str, required=True,
                        help='absolute path to price data root directory')

    parser.add_argument('--start_date', type=str, required=True,
                        help='start date in format YYYY/MM/DD')

    parser.add_argument('--remove_old', action='store_true',
                        help='remove old data (flag)')

    return parser.parse_args()


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
    """Main entry point (registered as `binance-download-price` CLI)"""
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
