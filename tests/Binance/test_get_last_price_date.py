import unittest
import shutil
import tempfile
import pandas as pd
import numpy as np
import datetime as dt
import os
import sys
from pathlib import Path
import logging

# Add project root to path to allow imports
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

# Add third_party directory to path to allow importing Binance package
# We go up 3 levels: unit -> Binance -> third_party
third_party_dir = str(Path(__file__).resolve().parent.parent.parent)
sys.path.append(third_party_dir)

from Binance import helper
from Binance import config

# Setup logger for testing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestHelper")


class TestGetLastPriceDate(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()
        self.dest_dir = os.path.join(self.test_dir, "exchange=Binance", "symbol=TEST")
        os.makedirs(self.dest_dir, exist_ok=True)

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def test_get_last_price_date_with_datetime_index(self):
        # Create sample data with DatetimeIndex
        dates = pd.date_range(start='2020-01-01', periods=5, freq='min')
        df = pd.DataFrame({'close': range(5)}, index=dates)

        # Save as parquet
        file_path = os.path.join(self.dest_dir, "part.0.parquet")
        df.to_parquet(file_path, engine='pyarrow')

        parquets_ary = [Path(file_path)]

        # Run function
        last_date = helper.get_last_price_date(self.dest_dir, parquets_ary, logger)

        # Check result
        # The stored parquet typically is naive or UTC depending on how pandas saves it.
        # pandas to_parquet usually preserves timezone if present, or is naive.
        # Here we created naive timestamps. They are saved as naive (usually implies local or no tz).
        # helper assumes naive means UTC.
        # 2020-01-01 00:04:00 naive -> assumed UTC -> converted to TW (+8)
        # expected: 2020-01-01 08:04:00+08:00

        expected_utc = dates[-1].replace(tzinfo=dt.timezone.utc)
        expected_tw = expected_utc.astimezone(helper.tw_tz)

        self.assertEqual(last_date, expected_tw)

    def test_get_last_price_date_with_date_column(self):
        # Create sample data with date column
        dates = pd.date_range(start='2020-01-01', periods=5, freq='min')
        df = pd.DataFrame({'date': dates, 'close': range(5)})
        # Reset index to default RangeIndex

        # Save as parquet
        file_path = os.path.join(self.dest_dir, "part.0.parquet")
        df.to_parquet(file_path, engine='pyarrow', index=False)

        parquets_ary = [Path(file_path)]

        # Run function
        last_date = helper.get_last_price_date(self.dest_dir, parquets_ary, logger)

        expected_utc = dates[-1].replace(tzinfo=dt.timezone.utc)
        expected_tw = expected_utc.astimezone(helper.tw_tz)

        self.assertEqual(last_date, expected_tw)

    def test_get_last_price_date_with_integer_timestamp(self):
        # Create sample data with integer timestamps (ms)
        # 2020-01-01 00:00:00 UTC = 1577836800000 ms
        base_ts = 1577836800000
        timestamps = [base_ts + i * 60000 for i in range(5)]

        df = pd.DataFrame({'close': range(5)}, index=timestamps)
        df.index.name = 'date'

        # Save as parquet
        file_path = os.path.join(self.dest_dir, "part.0.parquet")
        df.to_parquet(file_path, engine='pyarrow')

        parquets_ary = [Path(file_path)]

        # Run function
        last_date = helper.get_last_price_date(self.dest_dir, parquets_ary, logger)

        expected_dt = pd.to_datetime(timestamps[-1], unit='ms').replace(tzinfo=dt.timezone.utc)
        expected_tw = expected_dt.astimezone(helper.tw_tz)

        self.assertEqual(last_date, expected_tw)


if __name__ == '__main__':
    unittest.main()
