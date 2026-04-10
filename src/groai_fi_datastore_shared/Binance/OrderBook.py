import os
import time
import signal
import pandas as pd
from .config import BinanceConfig, get_default_config
from .utils import readable_error
from binance.client import Client as BinanceAPI
# from api.Binance.BinanceAPI import BinanceAPI

curent_dir = os.path.dirname(os.path.realpath(__file__))

TRADE_TIMEOUT = int(app_config.TRADE_TIMEOUT)
MAX_RECECOVERY = 5


# ==== handle timeout START ====
def timeout_handler(signum, frame):
    raise Exception("fetcher OrderBook api timeout error")


# Register the signal function handler
signal.signal(signal.SIGALRM, timeout_handler)
# ==== handle timeout END ====


class OrderBook:
    def __init__(self, exch_mode, logger):
        try:
            # Binance
            self.exch_mode = exch_mode

            api = app_config.BINANCE_API_KEY
            self.client = BinanceAPI(api, app_config.BINANCE_API_SECRET, testnet=(exch_mode == "SpotTest"))
            self.fail_count = 0
            self.logger = logger

        except Exception as e:
            self.logger.error(f"[OrderBook] {readable_error(e, __file__)}")

    def binance_order_book(self, symbol):
        """
        取得 order book

        """
        while True:
            try:
                signal.alarm(TRADE_TIMEOUT)
                orders = self.client.get_order_book(symbol=symbol, limit=50)
                signal.alarm(0)

                if 'asks' not in orders:
                    raise Exception('fetcher binance_order_book symbol:{0}, msg: {1}'.format(symbol, orders['msg']))

                # column names
                cols = ['price', 'volume']

                # create dataframe
                asks = pd.DataFrame(orders['asks'], columns=cols)
                bids = pd.DataFrame(orders['bids'], columns=cols)

                re = {'ask': asks, 'bid': bids}

                time.sleep(1)
                return re

            except Exception as e:
                self.logger.warning(readable_error(e, __file__))
                self.logger.info('[OrderBook] Try to restart Binance Client...see error msg one line above')

                time.sleep(5)
                self.fail_count += 1

                if self.fail_count <= MAX_RECECOVERY:
                    self.client = BinanceAPI(app_config.BINANCE_API_KEY, app_config.BINANCE_API_SECRET)
                    self.logger.info('[OrderBook] Binance Client started successfully...'
                                     'failed: {0} times'.format(self.fail_count))
                    continue
                else:
                    raise Exception('[OrderBook] Binance failed too many times:{0}'.format(self.fail_count))
