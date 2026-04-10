import os
import json
import time
from pathlib import Path
from .config import parquet_engine, tw_tz
from . import schema
from datetime import datetime
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import pandas as pd
import numpy as np
from typing import Optional
from . import utils
import datetime as dt


class FileLock:
    def __init__(self, lock_file: Path, logger, max_wait_time: int = 1800, poll_interval: int = 5,
                 safety_buffer: int = 10):
        self.lock_file = lock_file
        self.logger = logger
        self.max_wait_time = max_wait_time
        self.poll_interval = poll_interval
        self.safety_buffer = safety_buffer

    def acquire(self):
        start_time = time.time()

        # Check for stale lock file
        if self.lock_file.exists():
            try:
                content = ""
                try:
                    with open(self.lock_file, 'r') as f:
                        content = f.read().strip()
                except Exception:
                    pass

                lock_time = None
                timestamp_str = "unknown"

                # 1. Try JSON format
                try:
                    data = json.loads(content)
                    timestamp_str = data.get("timestamp")
                    if timestamp_str:
                        lock_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                except (json.JSONDecodeError, TypeError):
                    # 2. Try Legacy text format
                    if "at " in content:
                        try:
                            timestamp_str = content.split("at ")[-1]
                            lock_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                        except Exception:
                            pass

                # 3. Fallback to file modification time
                if lock_time is None:
                    try:
                        mtime = self.lock_file.stat().st_mtime
                        lock_time = datetime.fromtimestamp(mtime)
                        timestamp_str = lock_time.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        pass

                # Check if stale
                if lock_time:
                    lock_age = (datetime.now() - lock_time).total_seconds()
                    if lock_age > self.max_wait_time:
                        self.logger.warning(f"Found stale/invalid lock file at {self.lock_file}. "
                                            f"Content: '{content[:50]}...'. "
                                            f"Timestamp: {timestamp_str} (age: {lock_age}s > {self.max_wait_time}s). Removing it.")
                        try:
                            self.lock_file.unlink()
                        except FileNotFoundError:
                            pass  # Already removed by another process
            except Exception as e:
                self.logger.error(f"Error checking stale lock file at {self.lock_file}: {e}")

        # If lock exists, wait for it to be released
        while self.lock_file.exists():
            elapsed_time = time.time() - start_time
            if elapsed_time > self.max_wait_time:
                self.logger.error(
                    f"Timeout waiting for lock release on {self.lock_file} after {self.max_wait_time} seconds.")
                raise TimeoutError(f"Could not acquire lock on {self.lock_file}")

            self.logger.info(f"Lock file exists for {self.lock_file}. Waiting... Elapsed: {int(elapsed_time)}s")
            time.sleep(self.poll_interval)

        # After lock is gone, wait a safety buffer period to ensure data is fully written
        if self.safety_buffer > 0:
            self.logger.info(f"Lock for {self.lock_file} is gone. Waiting {self.safety_buffer}s for stabilization...")
            time.sleep(self.safety_buffer)

        # Create our own lock file now
        with open(self.lock_file, 'w') as f:
            json.dump({
                "pid": os.getpid(),
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "message": f"Locked by process {os.getpid()}"
            }, f)

    def release(self):
        if self.lock_file.exists():
            try:
                self.lock_file.unlink()
            except Exception as e:
                self.logger.error(f"Failed to remove lock file {self.lock_file}: {e}")

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


def to_unixmillis(from_date):
    from_date_obj = datetime.strptime(from_date, schema.datefmt)
    past = datetime(1970, 1, 1, tzinfo=from_date_obj.tzinfo)
    return int((from_date_obj - past).total_seconds() * 1000.0)


def to_unixmillis_datetime(from_date):
    from_date_obj = datetime.strptime(from_date, schema.datetimefmt)
    past = datetime(1970, 1, 1, tzinfo=from_date_obj.tzinfo)
    return int((from_date_obj - past).total_seconds() * 1000.0)


def to_datetime(ms):
    # https://stackoverflow.com/questions/69652691/python-incorrect-date-when-converting-unix-time-to-utc-time
    # https://www.unixtimestamp.com/index.php
    # https://avilpage.com/2014/11/python-unix-timestamp-utc-and-their.html
    a = datetime.utcfromtimestamp(int(float(ms) / 1000.0))
    origin = datetime.fromtimestamp(int(float(ms) / 1000.0)).astimezone(tw_tz)
    # TODO fix this timezone problem
    return origin


def save_price_parquet(data, dest_dir, append=True, overwrite=False, n_partitions=None):
    """
    Save price data to a partitioned Parquet dataset with consistent schema handling.

    Parameters:
    -----------
    data : pandas.DataFrame
        Price data to save. Must have 'date' as index or column.
    dest_dir : str
        Destination directory for Parquet files (already includes exchange/symbol partition)
        Format: /path/to/data/exchange=Binance/symbol=BTCUSDT
    append : bool, default True
        Whether to append to existing data
    overwrite : bool, default False
        Whether to overwrite existing data (only relevant if append=False)
    n_partitions : int, optional
        Number of Dask partitions to use. If None, auto-calculate based on data size.
        For append operations, this is ignored to maintain existing partition structure.
    
    Returns:
    --------
    bool : True if successful
    
    Notes:
    ------
    - Ensures consistent schema between writes (date field always in same position)
    - Avoids __null_dask_index__ by properly naming and handling the date index
    - For appends, maintains existing partition structure to avoid imbalanced files
    - Uses Hive-style partitioning: exchange=X/symbol=Y/part.*.parquet
    """
    print("Binance/save_price_parquet")
    import re
    import glob

    # Extract partition information from the directory path
    match = re.search(r'(exchange=[^/]+)/(symbol=[^/]+)', dest_dir)

    if not match:
        raise Exception("dest_dir must follow Hive partition format: .../exchange=XXX/symbol=YYY")

    exchange_part = match.group(1)
    symbol_part = match.group(2)

    # Find the root directory (parent of the exchange/symbol structure)
    root_dir = dest_dir.split(exchange_part)[0].rstrip('/')

    # Get just the exchange and symbol values (for DataFrame columns)
    exchange = exchange_part.split('=')[1]
    symbol = symbol_part.split('=')[1]

    print(f"Extracted partition info: exchange={exchange}, symbol={symbol}")
    print(f"Root Parquet directory: {root_dir}")
    print(f"Partition directory: {dest_dir}")

    # Make sure partition directory exists
    os.makedirs(dest_dir, exist_ok=True)

    # ===== STEP 1: Prepare DataFrame with consistent schema =====
    df = data.copy()

    # Ensure partition columns exist
    if 'exchange' not in df.columns:
        df['exchange'] = exchange
    if 'symbol' not in df.columns:
        df['symbol'] = symbol

    # Ensure date is the index with proper name
    if df.index.name != 'date':
        if 'date' in df.columns:
            df.set_index('date', inplace=True)
        else:
            raise ValueError("DataFrame must have 'date' as index or column")
    
    # Ensure index name is set (prevents __null_dask_index__)
    df.index.name = 'date'

    # Apply schema types (excluding date which is the index)
    type_dict = {k: v for k, v in schema.price_parquet.items() if k != 'date'}
    for col in type_dict:
        if col in df.columns:
            df[col] = df[col].astype(type_dict[col])

    # Ensure consistent column order (important for schema consistency)
    # Order: yymm, exchange, symbol, open, high, low, close, volume
    column_order = [c for c in schema.price_header_parquet if c != 'date' and c in df.columns]
    df = df[column_order]

    print(f"DataFrame prepared: shape={df.shape}, index={df.index.name}, columns={list(df.columns)}")

    # ===== STEP 2: Handle existing data alignment =====
    existing_files = glob.glob(f"{dest_dir}/part.*.parquet")
    has_existing_data = len(existing_files) > 0

    if append and has_existing_data:
        try:
            # Read existing data to align schema
            existing_dd = dd.read_parquet(dest_dir, engine=parquet_engine)
            
            # Align column dtypes
            for col in df.columns:
                if col in existing_dd.columns:
                    target_dtype = existing_dd[col].dtype
                    if df[col].dtype != target_dtype:
                        print(f"Aligning column '{col}' dtype: {df[col].dtype} -> {target_dtype}")
                        df[col] = df[col].astype(target_dtype)
            
            # Align index dtype
            if existing_dd.index.dtype != df.index.dtype:
                print(f"Aligning index dtype: {df.index.dtype} -> {existing_dd.index.dtype}")
                # For datetime index, this should be safe
                df.index = df.index.astype(existing_dd.index.dtype)
            
            # For append, calculate n_partitions based on existing + new data size
            if n_partitions is None:
                existing_npartitions = existing_dd.npartitions
                new_rows = len(df)
                existing_rows_per_partition = len(existing_dd) / existing_npartitions if existing_npartitions > 0 else 100000
                # Add partitions proportional to new data
                additional_partitions = max(1, int(new_rows / existing_rows_per_partition))
                n_partitions = min(additional_partitions, 10)  # Cap at 10 new partitions per append
                print(f"Auto-calculated n_partitions for append: {n_partitions} (existing: {existing_npartitions})")
            
        except Exception as ex:
            print(f"Warning: Failed to align with existing dataset: {ex}")
            if n_partitions is None:
                n_partitions = 1
    else:
        # For new data or overwrite, calculate based on data size
        if n_partitions is None:
            rows = len(df)
            # Target ~100k rows per partition
            n_partitions = max(1, min(rows // 100000, 20))
            print(f"Auto-calculated n_partitions for new data: {n_partitions} (rows: {rows})")

    # ===== STEP 3: Convert to Dask DataFrame =====
    if isinstance(df, dd.DataFrame):
        data_dd = df
    else:
        # Use npartitions directly in from_pandas for better control
        data_dd = dd.from_pandas(df, npartitions=n_partitions)

    print(f"Dask DataFrame created: npartitions={data_dd.npartitions}")

    # ===== STEP 4: Handle overwrite mode =====
    if overwrite:
        if os.path.exists(dest_dir):
            import shutil
            print(f"Overwrite requested: Removing partition directory {dest_dir}")
            shutil.rmtree(dest_dir)
            os.makedirs(dest_dir, exist_ok=True)
        append = False

    # ===== STEP 5: Write to Parquet =====
    try:
        kwargs = {
            'compression': 'snappy',
            'engine': parquet_engine,
            'append': append,
            'overwrite': False,  # Never use overwrite=True with partitions (would wipe root)
            'write_index': True,
            'partition_on': ['exchange', 'symbol'],
            'name_function': lambda i: f"part.{i:05d}.parquet",  # Consistent naming
        }

        # Only add ignore_divisions when appending
        if append:
            kwargs['ignore_divisions'] = True

        print(f"Writing to Parquet: append={append}, npartitions={data_dd.npartitions}")

        # Write data to root Parquet directory (creates proper partition structure)
        data_dd.to_parquet(root_dir, **kwargs)
        
        print(f"Successfully wrote data to {root_dir}")
        print(f"Partition {dest_dir} now has {len(glob.glob(f'{dest_dir}/part.*.parquet'))} parquet files")

        return True

    except Exception as e:
        error_msg = str(e)
        
        # Handle schema mismatch with fallback strategy
        if "Appended dtypes differ" in error_msg or "Schema mismatch" in error_msg:
            if append:
                print(f"Schema mismatch detected. Falling back to Read-Concat-Rewrite strategy.")
                try:
                    # Read existing data
                    existing_dd = dd.read_parquet(dest_dir, engine=parquet_engine)
                    
                    # Concatenate with new data
                    combined_dd = dd.concat([existing_dd, data_dd], interleave_partitions=False)
                    
                    # Remove duplicates (by index)
                    combined_dd = combined_dd.reset_index().drop_duplicates(subset='date', keep='last').set_index('date')
                    
                    # Sort by index
                    combined_dd = combined_dd.map_partitions(lambda x: x.sort_index())
                    
                    # Repartition for balance
                    target_partitions = max(10, combined_dd.npartitions // 2)
                    combined_dd = combined_dd.repartition(npartitions=target_partitions)
                    
                    print(f"Rewriting partition with combined dataset ({combined_dd.npartitions} partitions)...")

                    # Remove old partition directory
                    import shutil
                    shutil.rmtree(dest_dir)
                    os.makedirs(dest_dir, exist_ok=True)

                    # Write combined data
                    combined_dd.to_parquet(
                        root_dir,
                        compression='snappy',
                        engine=parquet_engine,
                        append=False,
                        overwrite=False,
                        write_index=True,
                        partition_on=['exchange', 'symbol'],
                        name_function=lambda i: f"part.{i:05d}.parquet"
                    )

                    print("Fallback rewrite successful.")
                    return True
                    
                except Exception as e2:
                    print(f"Fallback failed: {e2}")
                    import traceback
                    traceback.print_exc()
                    raise e2

        print(f'Error saving data: {e}')
        import traceback
        traceback.print_exc()
        raise


def create_metadata_file(parquet_dir):
    """Create a _metadata file for a partitioned Parquet dataset"""
    import pyarrow.parquet as pq
    import glob
    import os

    print(f"Creating _metadata file for {parquet_dir}...")

    # Find all parquet files
    parquet_files = glob.glob(f"{parquet_dir}/**/*.parquet", recursive=True)

    if not parquet_files:
        print(f"No parquet files found in {parquet_dir}")
        return False

    print(f"Found {len(parquet_files)} parquet files")

    try:
        # Create the metadata file
        metadata_path = os.path.join(parquet_dir, "_metadata")

        # Use pyarrow to write the metadata file
        pq.write_metadata(
            pq.read_schema(parquet_files[0]),
            metadata_path
        )

        print(f"Created _metadata file at {metadata_path}")
        return True
    except Exception as e:
        print(f"Error creating metadata file: {e}")
        import traceback
        traceback.print_exc()
        return False


def repartition(dest_dir):
    try:
        with ProgressBar():
            data_dd = dd.read_parquet(dest_dir, engine=parquet_engine, aggregate_files=True)
            data_dd.repartition(npartitions=data_dd.npartitions // 10 + 1) \
                .to_parquet(dest_dir, compression='snappy', engine=parquet_engine, partition_on=['exchange', 'symbol'])
    except Exception as e:
        print('Error saving data: {0}'.format(e))
        raise


def load_base_price(exchange, symbol, price_data_path, interval_base, cols=None, index=None):
    """
    Load base price data from parquet files
    
    Args:
        exchange: Exchange name (e.g., "Binance")
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        price_data_path: Path to price data directory
        interval_base: Base interval (e.g., "1m")
        cols: Columns to load (optional)
        index: Index column (optional)
    
    Returns:
        Dask DataFrame with price data
    """
    import time
    import datetime as dt
    from dask.diagnostics import ProgressBar

    try:
        start_time = time.time()

        # Build directory path
        dest_dir = f"{price_data_path}/exchange={exchange}/symbol={symbol}"

        print(f"[load_base_price] Reading from: {dest_dir}")

        # Read the data
        with ProgressBar():
            data_dd = dd.read_parquet(
                dest_dir,
                engine=parquet_engine,
                columns=cols,
                index=index if index else False,
                aggregate_files=True
            ).persist()

        end_time = time.time() - start_time
        duration_str = str(dt.timedelta(seconds=end_time))
        print(f"[{exchange}] {symbol} load_base_price takes {duration_str} to complete")

        return data_dd

    except Exception as e:
        from .utils import readable_error
        err = readable_error(e, __file__)
        print(f"Error load_base_price: {err}")
        return None


def get_last_price_date(dest_dir, parquets_ary, logger) -> Optional[datetime]:
    """
    Find the last available datetime in the existing parquet data.
    """
    try:
        # Determine if 'date' is a column or index by inspecting the first file
        # This avoids reading unnecessary data or guessing
        with ProgressBar():
            sample_df = pd.read_parquet(parquets_ary[0])

        has_date_column = 'date' in sample_df.columns
        columns_to_read = ['date'] if has_date_column else ['close']

        with ProgressBar():
            data_origin_dd = dd.read_parquet(
                dest_dir,
                columns=columns_to_read,
                engine=parquet_engine,
                aggregate_files=True
            )

        computed_max = None
        if has_date_column:
            # If date is a column, use it
            computed_max = data_origin_dd['date'].max().compute()
        elif data_origin_dd.index.name == 'date' or isinstance(sample_df.index, pd.DatetimeIndex):
            # If index is date or datetime index, use index
            computed_max = data_origin_dd.index.max().compute()

        last_datetime = None
        if computed_max is not None:
            if hasattr(computed_max, 'to_pydatetime'):
                last_datetime = computed_max.to_pydatetime()
            elif isinstance(computed_max, (pd.Timestamp, datetime)):
                last_datetime = computed_max
            elif isinstance(computed_max, np.datetime64):
                last_datetime = pd.to_datetime(computed_max).to_pydatetime()
            else:
                # Fallback for integer timestamps
                try:
                    val_int = int(computed_max)
                    if val_int > 1e16:  # ns
                        last_datetime = pd.to_datetime(val_int, unit='ns').to_pydatetime()
                    elif val_int > 1e11:  # ms
                        last_datetime = pd.to_datetime(val_int, unit='ms').to_pydatetime()
                    elif val_int > 0:
                        last_datetime = pd.to_datetime(val_int, unit='s').to_pydatetime()
                except Exception:
                    pass

        if last_datetime:
            # Handle Timezone - normalize to UTC
            if last_datetime.tzinfo is None:
                # Assume UTC for naive timestamps from parquet
                last_datetime = last_datetime.replace(tzinfo=dt.timezone.utc)

            last_datetime_utc = last_datetime.astimezone(dt.timezone.utc)

            return last_datetime_utc

    except Exception as e:
        logger.error(f"Error determining last date from parquet: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return None
