"""
Shared utilities for Binance entry scripts
"""
import time
import shutil
import datetime as dt


def copy_dir(source_dir, destination_dir, logger):
    """Copy directory from source to destination"""
    start_time = time.time()
    shutil.copytree(source_dir, destination_dir)
    logger.info(f'copy_dir takes {str(dt.timedelta(seconds=(time.time() - start_time)))}')


def del_dir(destination_dir, logger):
    """Delete directory"""
    shutil.rmtree(destination_dir)
    logger.info(f'del_dir {destination_dir} executed')
