import os
import time
import sys
import coloredlogs, logging
coloredlogs.install()
import numpy as np
from .utils import d, readable_error, get_project_root

# Stub for load_price - implement based on your needs
def load_price(*args, **kwargs):
    raise NotImplementedError("load_price needs to be implemented or imported from appropriate module")
from logging.handlers import TimedRotatingFileHandler


def load_data(exchange: str,
              symbol: str,
              price_data_path: str,
              trade_interval: str,
              interval_base: str,
              col: str,
              logger: logging.Logger):
    try:
        if price_data_path[:2] == "./":
            price_data_path = f"{get_project_root()}/{price_data_path}"

        price_dd, prices_base = load_price(exchange,
                                              symbol,
                                              price_data_path,
                                              trade_interval,
                                              interval_base,
                                              logger)
        price_trade = price_dd.compute()
        # price_trade = pd.DataFrame(price_trade, columns=cols, index=klines.index)

        price_re = price_trade[col].apply(lambda x: d(x))
        price_re = price_re.to_numpy()
        dt_idx = np.array(price_trade.index.to_pydatetime(), dtype=np.datetime64)
        return dt_idx, price_re

    except Exception as e:
        err = readable_error(e, __file__)
        logger.error(err) if logger is not None else print(err)
        time.sleep(3)
        sys.exit()


def main():
    exchange = "Binance"
    symbol = "ETHUSDT"
    price_data_path = "appData/trainData_crypto/prices_v3.parquet"
    trade_interval = "10T"
    interval_base = "1T"
    col = "open"

    dt_idx, price_re = load_data(exchange,
                                 symbol,
                                 price_data_path,
                                 trade_interval,
                                 interval_base,
                                 col,
                                 logger
                                 )
    assert price_re is not None


if __name__ == '__main__':
    # logger
    LOGLEVEL = os.environ.get('LOGLEVEL', 'DEBUG')

    logger = logging.getLogger(__name__)
    logger.setLevel(LOGLEVEL)
    handler = TimedRotatingFileHandler(
        f'{get_project_root()}/logs/api_Binance_DataReadWrite.log', when="D", interval=1, backupCount=15,
        encoding="UTF-8",
        delay=False, utc=True
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)-8s][%(filename)s:%(funcName)s:%(lineno)d] - %(message)s")
    )
    handler.setLevel(LOGLEVEL)
    logger.addHandler(handler)

    main()
