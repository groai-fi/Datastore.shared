import os
import json
import sys
import time
import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from binance.client import Client
from . import schema
from . import helper
from . import utils
from .utils import readable_error
from .config import parquet_engine
from pathlib import Path
import dask.dataframe as dd
from dask.delayed import Delayed
from dask.diagnostics import ProgressBar
from typing import Final, Optional
from binance.client import Client as BinanceAPI

asset_columns = ['Symbol', 'base_asset', 'quote_asset', 'precision', 'permission', 'order_type']
# This API is for downloading data or price, so USE IS_LIVE and no need to add logic
client = BinanceAPI(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"), testnet=False)


def get_asset() -> list:
    """
    Get all product from Binance
    :return:
    """
    try:
        rows = client.get_exchange_info()['symbols']
        assets = []  # only asset symbol
        products = []  # all data
        for row in rows:
            if row['status'] == 'TRADING':
                products.append([row['symbol'], row['baseAsset'], row['quoteAsset'], row['quotePrecision'],
                                 ','.join(row['permissions']), ','.join(row['orderTypes'])])
                assets.append(row['symbol'])

    except Exception as e:
        err = readable_error(e, __file__)
        print(f"get_asset Exception: {err}")
        raise Exception(e)

    if len(products) == 0:
        raise Exception('empty source data, check your connection')

    products_pd = pd.DataFrame(products, columns=asset_columns)
    products_pd.updated = False
    products_pd.to_csv('model_data/binance_asset.csv', index=False)

    return assets


def download_data_from_binance(symbol, kline_tframe, from_date, to_date, step=1, pause=0):
    """
    :param symbol:
    :param from_date:
    :param to_date:
    :param kline_tframe:
    :param step: step in number of days. Download data in batches of days given by 'step'
    :param pause: pause seconds before downloading next batch.
        if pause == -1 --> random sleep(2,5)
        if pause == 0 --> no sleep
        if pause == num--> sleep for num of seconds
    :return:
    """

    from_date_obj = datetime.strptime(from_date, schema.datefmt)
    step_date_obj = from_date_obj + timedelta(days=step)
    step_date = step_date_obj.strftime(schema.datefmt)

    from_millis = helper.to_unixmillis(from_date)
    to_millis = helper.to_unixmillis(to_date)
    step_millis = helper.to_unixmillis(step_date)

    count = 0
    price_list = pd.DataFrame([], columns=schema.binance_columns)
    price_list = price_list.astype(dtype=schema.binance_product_columns)
    while True:
        from_millis_str = str(from_millis)
        step_millis_str = str(step_millis)

        sys.stdout.write('\rStep %d:Downloading %s data from %s to %s' % (count, symbol,
                                                                          str(helper.to_datetime(from_millis_str)),
                                                                          str(helper.to_datetime(step_millis_str))))
        sys.stdout.flush()

        # download data

        klines = client.get_klines(symbol, kline_tframe, from_millis_str, step_millis_str)
        klines_len = len(klines)
        if klines_len == 0:
            sys.stdout.write('\r   Failed to download from %s to %s. '
                             'Got %d' % (str(helper.to_datetime(from_millis_str)),
                                         str(helper.to_datetime(step_millis_str)),
                                         klines_len))
            sys.stdout.flush()
            # time.sleep(0)

        sys.stdout.write('\r   Downloaded {0} of len {1} from {2} to '
                         '{3}'.format(symbol, klines_len,
                                      str(helper.to_datetime(from_millis_str)),
                                      str(helper.to_datetime(step_millis_str))))
        sys.stdout.flush()

        data_df = pd.DataFrame(klines, columns=schema.binance_columns)
        data_df = data_df.astype(dtype=schema.binance_product_columns)
        data_df['date'] = pd.to_datetime(data_df['date'], unit='ns')  # 'ms'
        # data_df.set_index('date', inplace=True)
        price_list = price_list.append(data_df, ignore_index=True)

        # move to next step of batches
        from_millis = step_millis
        step_date_obj = step_date_obj + timedelta(days=step)
        step_date = step_date_obj.strftime(schema.datefmt)
        step_millis = helper.to_unixmillis(step_date)
        count = count + 1
        if pause == -1:
            pause = np.random.rand(1)
        time.sleep(pause)
        if step_millis >= to_millis:
            break

    # price_list.set_index('date', inplace=True)
    # price_list = price_list.reset_index().drop_duplicates(subset='date', keep='first').set_index('date').sort_index()

    return price_list


def download_data_from_binance_1minute(symbol, kline_tframe, from_date, to_date, logger,
                                       step=8, pause=-1, worker_id=None, progress_cb=None):
    """

    :param logger:
    :param symbol:
    :param from_date:
    :param to_date:
    :param kline_tframe:
    :param step: step in number of days. Download data in batches of days given by 'step'
    :param pause: pause seconds before downloading next batch.
        if pause == -1 --> random sleep(2,5)
        if pause == 0 --> no sleep
        if pause == num--> sleep for num of seconds
    :param progress_cb:
    :param worker_id:
    :return:
    """

    logger.info('[API] Start downloading {}'.format('Binance_{0}'.format(symbol)))

    # accept string ONLY and format to 'schema.datetimefmt' to make sure everything is correct
    from_date_obj = datetime.strptime(from_date, schema.datetimefmt)
    step_date_obj = from_date_obj + timedelta(hours=step)
    step_date = step_date_obj.strftime(schema.datetimefmt)

    from_millis = helper.to_unixmillis_datetime(from_date)
    to_millis = helper.to_unixmillis_datetime(to_date)
    step_millis = helper.to_unixmillis_datetime(step_date)

    count = 0
    price_list = pd.DataFrame([], columns=schema.binance_columns)
    price_list = price_list.astype(dtype=schema.binance_columns_type)

    original_from_millis = from_millis
    original_to_millis = to_millis
    logger.info(f'[API] download_data_from_binance_1minute download range: {from_date} ~ {to_date}')

    while True:
        # progress
        progress = (step_millis - original_from_millis) / (original_to_millis - original_from_millis)
        if progress_cb and worker_id:
            progress_cb((worker_id, int(progress * 100)))

        if progress == 1:
            return price_list

        # start
        from_millis_str = str(from_millis)
        step_millis_str = str(step_millis)

        # download data
        klines = client.get_klines(symbol=symbol,
                                   interval=kline_tframe,
                                   startTime=from_millis,
                                   endTime=step_millis)
        if type(klines) is not list:
            if klines['code'] is not None and klines['msg'] is not None:
                msg = 'error downloading Binance price:{0}, {1}, ' \
                      'interval given:'.format(klines['code'], klines['msg'], kline_tframe)
                print(msg)
                raise ValueError(msg)

        klines_len = len(klines)
        if klines_len == 0:
            break

        start_tw_str = str(helper.to_datetime(from_millis_str))
        last_tw_str = str(helper.to_datetime(step_millis_str))

        logger.info(f'[API] Executing download range: {start_tw_str}~{last_tw_str}')

        msg = '\t Downloaded {0} of len {1} from {2} to ' \
              '{3}\r'.format(symbol, klines_len, start_tw_str, last_tw_str)

        sys.stdout.write(msg)
        sys.stdout.flush()

        data_df = pd.DataFrame(klines, columns=schema.binance_columns)
        data_df = data_df.astype(dtype=schema.binance_columns_type)
        # IMPORTANT: after converting to datetime. it is changed to UTC datetime
        data_df['date'] = pd.to_datetime(data_df['date'], unit='ms', origin='unix')  # UTC
        # if app_config.store_as_utc_local:
        #     data_df['date'] = data_df['date'].dt.tz_localize('UTC').dt.tz_convert(app_config.time_zone)

        # data_df.set_index('date', inplace=True)
        price_list = pd.concat([price_list, data_df], ignore_index=True)

        # move to next step of batches
        from_millis = step_millis
        step_date_obj = step_date_obj + timedelta(hours=step)
        step_date = step_date_obj.strftime(schema.datetimefmt)
        step_millis = helper.to_unixmillis_datetime(step_date)

        # check the next next step
        # step_date_obj2 = step_date_obj + timedelta(hours=step)
        # step_date2 = step_date_obj2.strftime(schema.datefmt)
        # step_millis2 = helper.to_unixmillis(step_date2)

        # if step_millis2 > to_millis > step_millis:
        #    step_millis = to_millis

        count = count + 1
        if pause == -1 and klines_len > 0:
            pause = np.random.rand()
        elif klines_len == 0:
            pause = 0

        # bursting if
        if count > 3:
            time.sleep(pause)
        # if step_millis > to_millis:
        #     break

    # price_list.set_index('date', inplace=True)
    # price_list = price_list.reset_index().drop_duplicates(subset='date', keep='first').set_index('date').sort_index()

    return price_list


def is_price_table_exist(exchange, asset, price_root_dir) -> bool:
    # price_file = helper.get_parquet_filename(
    #    '{0}/exchange={1}/symbol={2}/part.*.parquet'.format(dir, exchange, asset))
    # is_existed = glob.glob(price_file)
    # return len(is_existed) > 0
    if price_root_dir[:2] == "./" or price_root_dir[:8] == "appData/":
        price_root_dir = f"{utils.get_project_root()}/{price_root_dir}"

    root_dir_exist = utils.is_dir_exist(price_root_dir)
    if root_dir_exist:
        # slash is to navigate Reference: https://docs.python.org/3/library/pathlib.html
        sibling = 'exchange={0}/symbol={1}/'.format(exchange, asset)
        month_folder = Path(price_root_dir).expanduser() / sibling
        list_of_month_folder = list(month_folder.glob('part.*.parquet'))
        return len(list_of_month_folder) > 0
    return False


def catchup_price_binance(symbol, kline_tframe, default_download_start_date, price_root_dir, logger,
                          worker_id='', progress_cb=None, verbose=True):
    """
    看看是否已經有檔案存在，有的話就繼續下載，沒有的話用預設的日期去下載
    Get price for single crypto pair
    :param logger:
    :param verbose:
    :param price_root_dir: 在 project 中的一個檔案位置
    :param default_download_start_date:
    :param kline_tframe:
    :param worker_id:
    :param progress_cb:
    :param symbol: symbol of the pair, such as ETHBTC
    :param update_mode: DEPRECATED ‘replace’, ‘append’. https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
    :return: save to database tables
    """
    # create table if not existed
    # table_name = '{0}_{1}'.format(table_name, kline_tframe)

    time_before = time.time()
    exchange: Final = "Binance"

    if isinstance(default_download_start_date, str):
        default_download_start_date = datetime.strptime(default_download_start_date, '%Y-%m-%d')

    # Check if it is a new download or a catch-up
    existed = is_price_table_exist(exchange, symbol, price_root_dir)
    logger.info(
        f"{__file__}.catchup_price_binance file exist:{existed}, symbol:{symbol}, price_root_dir:{price_root_dir}")
    if not existed:
        logger.info("New symbol download detected. Proceeding...")
        # logger.error("Price file must exist in order to proceed")
        # time.sleep(3)
        # sys.exit()

    # folder location
    dest_dir = f'{utils.get_project_root()}/{price_root_dir}/exchange={exchange}/symbol={symbol}'
    parquet_folder = Path(dest_dir).expanduser()

    # Create destination directory if it doesn't exist
    parquet_folder.mkdir(parents=True, exist_ok=True)

    # Define lock file path
    lock_file = parquet_folder / ".write.lock"

    # Try to acquire lock
    try:
        with helper.FileLock(lock_file, logger):
            # Continue with the rest of the function that downloads data and appends to a file

            # 看是否已經有資料存在
            last_datetime_utc: Optional[datetime] = None
            almost_now = datetime.today() + timedelta(minutes=1)
            if not existed:
                start_date = default_download_start_date.replace(tzinfo=dt.timezone.utc)
                parquets_ary = ()
            else:
                # filters = [('exchange', '==', exchange'), ('symbol', '==', symbol)]

                # =============================
                # This code is slower, we replace it with from_map
                # with warnings.catch_warnings():
                #     warnings.simplefilter("ignore")
                #    data_origin_dd = dd.read_parquet(dest_dir, columns=['close'],
                #                                     engine=parquet_engine,
                #                                     aggregate_files=True)

                # Read from ALL partition files to find the true maximum date
                # After merging/repartitioning, data is distributed across partitions,
                # so the last numbered partition might not contain the maximum date
                parquets_ary = list(parquet_folder.glob('part.*.parquet'))

                # Initialize start_date with default (as UTC aware)
                start_date = default_download_start_date.replace(tzinfo=dt.timezone.utc)

                if len(parquets_ary) > 0:
                    last_datetime_utc = helper.get_last_price_date(dest_dir, parquets_ary, logger)
                    user_start_utc = default_download_start_date.replace(tzinfo=dt.timezone.utc)
                    start_date = user_start_utc  # default
                    if last_datetime_utc:
                        start_date = last_datetime_utc + timedelta(minutes=1)

                    # Ensure user start date is also UTC aware for comparison
                    logger.info(f"Last data time (UTC): {last_datetime_utc}")
                    logger.info(f"User default start (UTC): {user_start_utc}")
                    logger.info(f"Final start (UTC): {start_date}")

            # 開始做下載的動作

            # datetime.strptime(dateS, "%d/%m/%Y %H:%M:%S")
            # download_start_date = int(config.download_start_date.timestamp() * 1000)
            # download_end_date = int(config.download_end_date.timestamp() * 1000)
            # stock = client.get_klines(symbol, config.kline_tframe, download_start_date, download_end_date)

            # download_start_date = int(start_date.timestamp() * 1000)
            # now = datetime.now()
            # download_end_date = int(now.timestamp() * 1000)
            download_start_date = start_date.strftime(schema.datetimefmt)
            download_end_date = almost_now.strftime(schema.datetimefmt)
            days = utils.calculate_days_to_download(kline_tframe)

            # Sanitize start_date to prevent early exit if before listing
            try:
                # Use get_klines with startTime=0 to find the first available candle
                # This works across different library versions compared to get_earliest_valid_timestamp
                # startTime=0 (1970) will return the first candle for the symbol
                first_candle = client.get_klines(symbol=symbol, interval=kline_tframe, startTime=0, limit=1)

                if first_candle and len(first_candle) > 0:
                    earliest_ts = int(first_candle[0][0])  # Open time
                    earliest_dt = datetime.fromtimestamp(earliest_ts / 1000)

                    # check helper.to_unixmillis
                    start_ts = helper.to_unixmillis_datetime(download_start_date)

                    if start_ts < earliest_ts:
                        logger.info(
                            f"Requested start date {download_start_date} is before listing. Adjusting to earliest valid date: {earliest_dt}")
                        download_start_date = earliest_dt.strftime(schema.datetimefmt)
            except Exception as e:
                logger.warning(f"Failed to fetch earliest timestamp for {symbol}: {e}. Proceeding with original date.")

            logger.info(f'[API] catchup_price_binance {download_start_date} ~ {download_end_date}')
            if kline_tframe == '1m' or kline_tframe == '1T':
                stock_df = download_data_from_binance_1minute(symbol, kline_tframe, download_start_date,
                                                              download_end_date, logger, step=8,
                                                              worker_id=worker_id, progress_cb=progress_cb)
            else:
                stock_df = download_data_from_binance(symbol, kline_tframe, download_start_date, download_end_date,
                                                      step=days)
            """
              [
                1499040000000,      // Open time
                "0.01634790",       // Open
                "0.80000000",       // High
                "0.01575800",       // Low
                "0.01577100",       // Close
                "148976.11427815",  // Volume
                1499644799999,      // Close time
                "2434.19055334",    // Quote asset volume
                308,                // Number of trades
                "1756.87402397",    // Taker buy base asset volume
                "28.46694368",      // Taker buy quote asset volume
                "17928899.62484339" // Ignore.
              ]
            """
            if not stock_df.empty:
                # stock.insert(0, 'symbol', symbol)
                stock_df = stock_df.iloc[:, :6]
                stock_df.columns = schema.price_columns
                # stock_df['date'] = pd.to_datetime(stock_df['date']).astype(int)
                # stock_df['date'] = dd.to_datetime(stock_df['date'], format=schema.datefmtnano)
                stock_df['date'] = dd.to_datetime(stock_df['date'], unit='ns')

                stock_df['symbol'] = symbol
                stock_df['exchange'] = exchange
                # stock_df['interval'] = kline_tframe
                stock_df = stock_df[~stock_df.index.duplicated(keep='first')]

                stock_df['yymm'] = stock_df['date'].dt.strftime("%y%m")

                # Explicitly cast to string to match partitions if they exist as pyarrow strings
                stock_df['symbol'] = stock_df['symbol'].astype("string")
                stock_df['exchange'] = stock_df['exchange'].astype("string")
                stock_df['yymm'] = stock_df['yymm'].astype("string")

                # sequence is important
                stock_df = stock_df[schema.price_header_parquet]
                # folder location
                dest_dir = f'{utils.get_project_root()}/{price_root_dir}/exchange={exchange}/symbol={symbol}'

                # append only when there is original data exists
                is_append = len(parquets_ary) > 0 if last_datetime_utc is not None else False
                helper.save_price_parquet(stock_df, dest_dir, append=is_append)
                logger.info(f"[API] downloaded last row:\n{stock_df.iloc[-1].to_dict()}")
                # helper.repartition(dest_dir)

                if verbose:
                    duration = time.strftime("%H:%M:%S", time.gmtime(time.time() - time_before))
                    logger.info(f'[{exchange}] {symbol} Time to catchup price: '
                                f'{duration}')

                return stock_df
            else:
                logger.warning(f'[{exchange}] No data for {symbol}')
                return None

    except TimeoutError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f'get_klines Exception {symbol}: {e}')
        sys.exit(1)


def download_crypto_data(symbol, kline_tframe, start_date, price_root_dir, logger):
    """
    Get price for single crypto pair
    :param logger:
    :param price_root_dir: 在 project 中的一個檔案位置
    :param symbol: symbol of the pair, such as ETHBTC
    :param start_date:
    :param kline_tframe:

    :return:  save to parquet tables
    """

    exchange = 'Binance'

    try:
        download_start_date = start_date.strftime(schema.datefmt)
        almost_now = datetime.today() + timedelta(minutes=1)
        download_end_date = almost_now.strftime(schema.datefmt)
        days = utils.calculate_days_to_download(kline_tframe)

        logger.info(f'[API] download_crypto_data {download_start_date} ~ {download_end_date}\n'
                    f'overwriting existing data.....')
        if kline_tframe == '1m' or kline_tframe == '1T':
            data = download_data_from_binance_1minute(symbol, kline_tframe, download_start_date,
                                                      download_end_date, logger, step=8)
        else:
            data = download_data_from_binance(symbol, kline_tframe, download_start_date, download_end_date,
                                              step=days)
        """
          [
            1499040000000,      // Open time
            "0.01634790",       // Open
            "0.80000000",       // High
            "0.01575800",       // Low
            "0.01577100",       // Close
            "148976.11427815",  // Volume
            1499644799999,      // Close time
            "2434.19055334",    // Quote asset volume
            308,                // Number of trades
            "1756.87402397",    // Taker buy base asset volume
            "28.46694368",      // Taker buy quote asset volume
            "17928899.62484339" // Ignore.
          ]
        """
        if data.empty:
            # 沒有資料
            logger.warning('No data, please check your connection or input parameter to Binance')
            return None

        # 有資料，繼續
        data = data.iloc[:, :6]
        data['date'] = dd.to_datetime(data['date'], unit='ns')  # , format=schema.datefmtnano
        data['yymm'] = data['date'].dt.strftime("%y%m")
        data['symbol'] = symbol
        data['exchange'] = exchange

        # sequence is important
        data = data[schema.price_header_parquet]

        # folder location
        dest_dir = f'{utils.get_project_root()}/{price_root_dir}/exchange={exchange}/symbol={symbol}'

        helper.save_price_parquet(data, dest_dir,
                                  append=False,
                                  overwrite=True,
                                  n_partitions=10)

        return data

    except Exception as err:
        logger.error(f'get_klines Exception {symbol}: {err}')
