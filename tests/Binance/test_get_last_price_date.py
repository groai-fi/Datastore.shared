import unittest
import shutil
import tempfile
import pandas as pd
import datetime as dt
from pathlib import Path
import logging

# Package imports (no sys.path hacks needed when installed via `uv sync`)
from groai_fi_datastore_shared.Binance import helper
from groai_fi_datastore_shared.Binance.config import tw_tz

# Setup logger for testing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestHelper")


class TestGetLastPriceDate(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()
        self.dest_dir = str(Path(self.test_dir) / "exchange=Binance" / "symbol=TEST")
        Path(self.dest_dir).mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_get_last_price_date_with_datetime_index(self):
        # Create sample data with DatetimeIndex
        dates = pd.date_range(start='2020-01-01', periods=5, freq='min')
        df = pd.DataFrame({'close': range(5)}, index=dates)

        file_path = str(Path(self.dest_dir) / "part.0.parquet")
        df.to_parquet(file_path, engine='pyarrow')

        parquets_ary = [Path(file_path)]

        # Run function
        last_date = helper.get_last_price_date(self.dest_dir, parquets_ary, logger)

        # get_last_price_date normalises to UTC aware datetime.
        # Python datetime == compares moments in time, so UTC == TW+8 for the same instant.
        expected_utc = dates[-1].replace(tzinfo=dt.timezone.utc)
        self.assertEqual(last_date, expected_utc)

    def test_get_last_price_date_with_date_column(self):
        # Create sample data with date column
        dates = pd.date_range(start='2020-01-01', periods=5, freq='min')
        df = pd.DataFrame({'date': dates, 'close': range(5)})
        # Reset index to default RangeIndex

        file_path = str(Path(self.dest_dir) / "part.0.parquet")
        df.to_parquet(file_path, engine='pyarrow', index=False)

        parquets_ary = [Path(file_path)]

        # Run function
        last_date = helper.get_last_price_date(self.dest_dir, parquets_ary, logger)

        expected_utc = dates[-1].replace(tzinfo=dt.timezone.utc)
        self.assertEqual(last_date, expected_utc)

    def test_get_last_price_date_with_integer_timestamp(self):
        # Create sample data with integer timestamps (ms)
        # 2020-01-01 00:00:00 UTC = 1577836800000 ms
        base_ts = 1577836800000
        timestamps = [base_ts + i * 60000 for i in range(5)]

        df = pd.DataFrame({'close': range(5)}, index=timestamps)
        df.index.name = 'date'

        file_path = str(Path(self.dest_dir) / "part.0.parquet")
        df.to_parquet(file_path, engine='pyarrow')

        parquets_ary = [Path(file_path)]

        # Run function
        last_date = helper.get_last_price_date(self.dest_dir, parquets_ary, logger)

        expected_utc = pd.to_datetime(timestamps[-1], unit='ms').replace(tzinfo=dt.timezone.utc)
        self.assertEqual(last_date, expected_utc)


if __name__ == '__main__':
    unittest.main()
