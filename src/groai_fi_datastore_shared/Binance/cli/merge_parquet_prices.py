"""
Merge Binance parquet price files

Usage (installed CLI):
    binance-merge-parquet --exchange Binance --symbol BTCUSDT --path /data/prices_v3.parquet --interval_base 1m

Usage (direct):
    python -m groai_fi_datastore_shared.Binance.cli.merge_parquet_prices --exchange Binance ...
"""
import sys
import argparse
import datetime as dt

# Import from installed package
from groai_fi_datastore_shared.Binance import schema, helper
from groai_fi_datastore_shared.Binance.utils import readable_error
from groai_fi_datastore_shared.Binance.helper import load_base_price
from groai_fi_datastore_shared.Binance.cli.shared import copy_dir


def parse_arguments():
    parser = argparse.ArgumentParser(description='Merge Binance parquet price files')

    parser.add_argument('--exchange', type=str, required=True,
                        help='exchange (e.g. Binance)')

    parser.add_argument('--symbol', type=str, required=True,
                        help='symbol (e.g. BTCUSDT)')

    parser.add_argument('--path', type=str, required=True,
                        help='absolute path to price data root directory')

    parser.add_argument('--interval_base', type=str, required=True,
                        help='base interval (e.g. 1m)')

    return parser.parse_args()


def run():
    """Main entry point (registered as `binance-merge-parquet` CLI)"""
    import logging
    cmd_args = vars(parse_arguments())
    _null_logger = logging.getLogger(f"binance.merge.{cmd_args['symbol']}")
    _null_logger.addHandler(logging.NullHandler())

    now_str = dt.datetime.now().strftime('%Y%m%dT%H%M%S')

    price_dir = cmd_args['path']
    hive_dir = f"/exchange={cmd_args['exchange']}/symbol={cmd_args['symbol']}"
    price_dir_full = f"{price_dir}{hive_dir}"
    backup_dir = f"{price_dir_full}_{now_str}"

    # Load prices — read all columns without forcing index
    price_pd = load_base_price(
        cmd_args['exchange'],
        cmd_args['symbol'],
        price_dir,
        cmd_args['interval_base'],
        cols=None,   # Read all columns
        index=False  # Don't force index, read as-is
    )

    if price_pd is None:
        print("Error: Failed to load price data")
        sys.exit(1)

    # copy for backup
    try:
        copy_dir(price_dir_full, backup_dir, _null_logger)
        print(f'Backup → {backup_dir}')
    except Exception as e:
        err = readable_error(e, __file__)
        print(f"Error: {err}")
        sys.exit(1)

    try:
        print('Computing Dask DataFrame to pandas...')
        price_pd_computed = price_pd.compute()

        print(f'Loaded {len(price_pd_computed)} rows')
        print(f'Index: {price_pd_computed.index.name}')
        print(f'Columns: {list(price_pd_computed.columns)}')

        # Reset index if it's not named or is __null_dask_index__
        if price_pd_computed.index.name in [None, '__null_dask_index__']:
            print('Resetting unnamed/null index')
            price_pd_computed = price_pd_computed.reset_index(drop=True)

        # Ensure required columns exist
        if 'exchange' not in price_pd_computed.columns:
            price_pd_computed['exchange'] = cmd_args['exchange']
        if 'symbol' not in price_pd_computed.columns:
            price_pd_computed['symbol'] = cmd_args['symbol']

        # Ensure date column exists or is the index
        if price_pd_computed.index.name != 'date' and 'date' not in price_pd_computed.columns:
            print(f"Error: No 'date' column or index found in data")
            print(f"  Available columns: {list(price_pd_computed.columns)}")
            print(f"  Index name: {price_pd_computed.index.name}")
            sys.exit(1)

        # If date is a column, set it as index
        if 'date' in price_pd_computed.columns and price_pd_computed.index.name != 'date':
            print('Setting date column as index')
            price_pd_computed.set_index('date', inplace=True)

        print(f'Final DataFrame: index={price_pd_computed.index.name}, columns={list(price_pd_computed.columns)}')

        # Save merged data
        helper.save_price_parquet(
            price_pd_computed,
            price_dir_full,
            append=False,
            overwrite=True,
            n_partitions=10
        )
        print(f'✓ Successfully saved merged data to {price_dir_full}')

    except Exception as e:
        err = readable_error(e, __file__)
        print(f"Error: {err}")
        sys.exit(1)


if __name__ == '__main__':
    run()
