"""
Unit test for Binance download and merge functionality

Tests the complete flow:
1. Fresh download
2. Merge/compact
3. Gap fill
4. Multi-asset coexistence
"""
import unittest
import os
import shutil
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pq

# Package imports (no sys.path hacks needed when installed via `uv sync`)
from groai_fi_datastore_shared.Binance import BinanceMarketDataDownloader, helper
from groai_fi_datastore_shared.Binance.utils import setup_logger


class TestDownloadAndMerge(unittest.TestCase):
    """Test complete download and merge workflow"""

    @classmethod
    def setUpClass(cls):
        """Setup test environment"""
        # Use a temp directory so tests are fully isolated from the source tree
        cls._tmpdir = tempfile.mkdtemp(prefix="groai_test_dl_merge_")
        cls.test_data_dir = Path(cls._tmpdir)

        cls.symbol = "BCHUSDT"
        cls.exchange = "Binance"
        cls.tframe = "1m"
        cls.start_date = datetime.now() - timedelta(days=3)

        # price_root_dir must be absolute — the downloader no longer prepends a project root
        cls.price_root_dir = str(cls.test_data_dir)
        cls.symbol_dir = cls.test_data_dir / f"exchange={cls.exchange}" / f"symbol={cls.symbol}"

        # Logger writes to <cwd>/logs/ by default (or GROAI_LOG_DIR env var)
        cls.logger = setup_logger('test_download_merge.log', cls.symbol)

        print(f"\n[Setup] Test data directory: {cls.test_data_dir}")
        print(f"[Setup] Symbol directory: {cls.symbol_dir}")

    def setUp(self):
        """Clean test directory before each test"""
        print(f"\n[Setup] Cleaning {self.symbol_dir}...")
        if self.symbol_dir.exists():
            shutil.rmtree(self.symbol_dir)

    @classmethod
    def tearDownClass(cls):
        """Clean up temp directory after all tests"""
        print(f"\n[Cleanup] Removing test data directory: {cls.test_data_dir}")
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    def test_01_fresh_download(self):
        """Test fresh download from empty directory"""
        print(f"\n{'='*60}")
        print(f"TEST 1: Fresh Download")
        print(f"{'='*60}")
        print(f"Symbol: {self.symbol}")
        print(f"Start date: {self.start_date}")
        print(f"Data dir: {self.price_root_dir}")

        result = BinanceMarketDataDownloader.catchup_price_binance(
            symbol=self.symbol,
            kline_tframe=self.tframe,
            default_download_start_date=self.start_date,
            price_root_dir=self.price_root_dir,
            logger=self.logger
        )

        self.assertIsNotNone(result, "Download should return data")

        # Verify files exist
        self.assertTrue(self.symbol_dir.exists(), "Symbol directory should exist")
        files = list(self.symbol_dir.glob("part.*.parquet"))
        self.assertGreater(len(files), 0, "Should have at least one parquet file")

        print(f"✓ Created {len(files)} parquet files")

        # Verify schema
        self._check_schema(files[0])

        # Verify data
        df = pd.read_parquet(self.symbol_dir, engine='pyarrow')
        print(f"✓ Downloaded {len(df)} rows")
        self.assertGreater(len(df), 0, "Should have data")

    def test_02_merge_compact(self):
        """Test merge and compact functionality"""
        print(f"\n{'='*60}")
        print(f"TEST 2: Merge and Compact")
        print(f"{'='*60}")

        # First download
        BinanceMarketDataDownloader.catchup_price_binance(
            symbol=self.symbol,
            kline_tframe=self.tframe,
            default_download_start_date=self.start_date,
            price_root_dir=self.price_root_dir,
            logger=self.logger
        )

        files_before = list(self.symbol_dir.glob("part.*.parquet"))
        print(f"Before merge: {len(files_before)} files")

        # Load and merge
        price_dd = helper.load_base_price(
            exchange=self.exchange,
            symbol=self.symbol,
            price_data_path=self.price_root_dir,
            interval_base=self.tframe,
            cols=None,
            index=False
        )

        self.assertIsNotNone(price_dd, "Should load data")

        # Compute to pandas
        price_pd = price_dd.compute()

        # Reset index if needed
        if price_pd.index.name in [None, '__null_dask_index__']:
            price_pd = price_pd.reset_index(drop=True)

        # Ensure required columns
        if 'exchange' not in price_pd.columns:
            price_pd['exchange'] = self.exchange
        if 'symbol' not in price_pd.columns:
            price_pd['symbol'] = self.symbol

        # Set date as index
        if 'date' in price_pd.columns and price_pd.index.name != 'date':
            price_pd.set_index('date', inplace=True)

        # Save merged with more partitions
        helper.save_price_parquet(
            price_pd,
            str(self.symbol_dir),
            append=False,
            overwrite=True,
            n_partitions=10
        )

        files_after = list(self.symbol_dir.glob("part.*.parquet"))
        print(f"After merge: {len(files_after)} files")

        # Verify schema is still correct
        self._check_schema(files_after[0])

        # Verify data integrity
        df_after = pd.read_parquet(self.symbol_dir, engine='pyarrow')
        print(f"✓ Merged data: {len(df_after)} rows")
        self.assertGreater(len(df_after), 0, "Should have data after merge")

    def test_03_incremental_download(self):
        """Test incremental download (append mode)"""
        print(f"\n{'='*60}")
        print(f"TEST 3: Incremental Download")
        print(f"{'='*60}")

        BinanceMarketDataDownloader.catchup_price_binance(
            symbol=self.symbol,
            kline_tframe=self.tframe,
            default_download_start_date=self.start_date,
            price_root_dir=self.price_root_dir,
            logger=self.logger
        )

        df1 = pd.read_parquet(self.symbol_dir, engine='pyarrow')
        rows_initial = len(df1)
        print(f"Initial download: {rows_initial} rows")

        # Delete some recent files to simulate gap
        files = list(self.symbol_dir.glob("part.*.parquet"))
        if len(files) > 1:
            # Delete last file
            files.sort()
            to_delete = files[-1]
            print(f"Deleting {to_delete.name} to simulate gap...")
            os.remove(to_delete)

        BinanceMarketDataDownloader.catchup_price_binance(
            symbol=self.symbol,
            kline_tframe=self.tframe,
            default_download_start_date=self.start_date,
            price_root_dir=self.price_root_dir,
            logger=self.logger
        )

        df2 = pd.read_parquet(self.symbol_dir, engine='pyarrow')
        rows_after = len(df2)
        print(f"After incremental: {rows_after} rows")

        # Should have at least as many rows (might have more if new data arrived)
        self.assertGreaterEqual(rows_after, rows_initial * 0.9,
                               "Should recover most data after gap fill")

        print(f"✓ Gap fill successful")

    def test_04_multi_asset_isolation(self):
        """Test that multiple symbols don't interfere with each other"""
        print(f"\n{'='*60}")
        print(f"TEST 4: Multi-Asset Isolation")
        print(f"{'='*60}")

        # Download first symbol
        BinanceMarketDataDownloader.catchup_price_binance(
            symbol=self.symbol,
            kline_tframe=self.tframe,
            default_download_start_date=self.start_date,
            price_root_dir=self.price_root_dir,
            logger=self.logger
        )

        df1 = pd.read_parquet(self.symbol_dir, engine='pyarrow')
        rows1 = len(df1)
        print(f"{self.symbol}: {rows1} rows")

        # Download second symbol
        symbol2 = "ETHUSDT"
        symbol2_dir = self.test_data_dir / f"exchange={self.exchange}" / f"symbol={symbol2}"

        BinanceMarketDataDownloader.catchup_price_binance(
            symbol=symbol2,
            kline_tframe=self.tframe,
            default_download_start_date=self.start_date,
            price_root_dir=self.price_root_dir,
            logger=self.logger
        )

        df2 = pd.read_parquet(symbol2_dir, engine='pyarrow')
        rows2 = len(df2)
        print(f"{symbol2}: {rows2} rows")

        # Verify first symbol still exists and has data
        self.assertTrue(self.symbol_dir.exists(), f"{self.symbol} directory should still exist")
        df1_check = pd.read_parquet(self.symbol_dir, engine='pyarrow')
        self.assertEqual(len(df1_check), rows1, f"{self.symbol} data should be unchanged")

        # Verify second symbol exists
        self.assertTrue(symbol2_dir.exists(), f"{symbol2} directory should exist")
        self.assertGreater(rows2, 0, f"{symbol2} should have data")

        print(f"✓ Both symbols coexist independently")

        # Cleanup second symbol
        if symbol2_dir.exists():
            shutil.rmtree(symbol2_dir)

    def _check_schema(self, parquet_file):
        """Check that schema is correct (no __null_dask_index__)"""
        schema = pq.read_schema(parquet_file)
        field_names = [field.name for field in schema]

        print(f"\nSchema check for {parquet_file.name}:")
        print(f"  Fields: {field_names}")

        # Check for __null_dask_index__
        self.assertNotIn('__null_dask_index__', field_names,
                        "Schema should not contain __null_dask_index__")

        # Check for date field
        self.assertIn('date', field_names, "Schema should contain 'date' field")

        # Check for required price fields
        required_fields = ['open', 'high', 'low', 'close', 'volume']
        for field in required_fields:
            self.assertIn(field, field_names, f"Schema should contain '{field}' field")

        print(f"  ✓ Schema is valid")


if __name__ == '__main__':
    unittest.main(verbosity=2)
