import uuid
from .BinanceMarketDataDownloader import BinanceMarketDataDownloader
from .utils import readable_error


def get_asset():
    """
    Get all product from Binance
    :return:
    """
    try:
        assets = BinanceMarketDataDownloader.get_asset()
        # asset_model = AssetListModel('Binance')
        # asset_model.set_data(assets)
        print('Binance asset downloaded')
        return assets
    except Exception as e:
        err = readable_error(e, __file__)
        print(f"get_asset Exception: {err}")


def download_price(asset, interval, start_date, folder_name, logger,
                   file_mode='append', worker_id=None, progress_cb=None, verbose=True):
    # if not is_price_already_exist(exchange, asset, price_root_dir):
    try:
        if worker_id is None:
            worker_id = 111  # uuid.uuid4().hex

        interval = interval.replace('T', 'm')
        prices_df = BinanceMarketDataDownloader.catchup_price_binance(asset, interval, start_date, folder_name, logger, # file_mode,
                                                                      worker_id=worker_id, progress_cb=progress_cb, verbose=verbose)
        return prices_df
    except Exception as e:
        err = readable_error(e, __file__)
        logger.error(err)
        raise
