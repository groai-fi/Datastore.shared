import unittest
import os
import shutil
import tempfile
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from pathlib import Path

# Package imports (no sys.path hacks needed when installed via `uv sync`)
from groai_fi_datastore_shared.Binance import helper, schema
from groai_fi_datastore_shared.Binance import utils as utils_module


class TestSavePriceParquet(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.exchange = "Binance"
        cls.symbol = "TESTUSDT"

    def setUp(self):
        """Create a fresh temp directory before each test"""
        self._tmpdir = tempfile.mkdtemp(prefix="groai_test_parquet_")
        self.test_root = self._tmpdir
        self.dest_dir = f"{self.test_root}/exchange={self.exchange}/symbol={self.symbol}"

    def tearDown(self):
        """Remove temp directory after each test"""
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def create_sample_data(self, start_date, num_rows=100):
        """Create sample price data"""
        dates = pd.date_range(start=start_date, periods=num_rows, freq='1min')

        data = pd.DataFrame({
            'date': dates,
            'open': 100.0 + pd.Series(range(num_rows)) * 0.1,
            'high': 101.0 + pd.Series(range(num_rows)) * 0.1,
            'low': 99.0 + pd.Series(range(num_rows)) * 0.1,
            'close': 100.5 + pd.Series(range(num_rows)) * 0.1,
            'volume': 1000.0 + pd.Series(range(num_rows)) * 10,
            'yymm': dates.strftime('%y%m'),
            'exchange': self.exchange,
            'symbol': self.symbol
        })

        return data

    def read_parquet_files(self, dest_dir):
        """Read all parquet files from a directory and return combined DataFrame"""
        import glob
        parquet_files = glob.glob(f"{dest_dir}/part.*.parquet")
        if not parquet_files:
            return None

        dfs = [pd.read_parquet(f) for f in parquet_files]
        combined = pd.concat(dfs, ignore_index=False)
        return combined.sort_index()

    def check_schema_consistency(self, parquet_file):
        """Check if schema is consistent (no __null_dask_index__)"""
        schema_obj = pq.read_schema(parquet_file)
        field_names = [field.name for field in schema_obj]

        print(f"\nSchema fields: {field_names}")

        # Check for __null_dask_index__
        self.assertNotIn('__null_dask_index__', field_names,
                        "Schema should not contain __null_dask_index__")

        return field_names

    def test_01_initial_write(self):
        """Test initial write with overwrite=True"""
        print("\n" + "="*60)
        print("TEST 1: Initial Write")
        print("="*60)

        # Create sample data
        data = self.create_sample_data(start_date='2024-01-01 00:00:00', num_rows=150)

        result = helper.save_price_parquet(
            data,
            self.dest_dir,
            append=False,
            overwrite=True,
            n_partitions=None
        )

        self.assertTrue(result, "save_price_parquet should return True")

        # Verify files exist
        import glob
        parquet_files = glob.glob(f"{self.dest_dir}/part.*.parquet")
        self.assertGreater(len(parquet_files), 0, "Should create at least one parquet file")

        print(f"Created {len(parquet_files)} parquet files")

        # Check schema
        field_names = self.check_schema_consistency(parquet_files[0])

        # Read back and verify
        df_read = self.read_parquet_files(self.dest_dir)
        self.assertIsNotNone(df_read, "Should be able to read back data")
        self.assertEqual(len(df_read), 150, "Should have 150 rows")

        # Check index
        self.assertEqual(df_read.index.name, 'date', "Index should be named 'date'")

        print(f"✓ Initial write successful: {len(df_read)} rows")
        print(f"✓ Index name: {df_read.index.name}")
        print(f"✓ Columns: {list(df_read.columns)}")

    def test_02_append_data(self):
        """Test appending data to existing files"""
        print("\n" + "="*60)
        print("TEST 2: Append Data")
        print("="*60)

        # Step 1: Initial write
        data1 = self.create_sample_data(start_date='2024-01-01 00:00:00', num_rows=100)
        helper.save_price_parquet(
            data1,
            self.dest_dir,
            append=False,
            overwrite=True,
            n_partitions=None
        )

        import glob
        initial_files = glob.glob(f"{self.dest_dir}/part.*.parquet")
        print(f"Initial write: {len(initial_files)} files")

        # Get initial schema
        initial_schema_fields = self.check_schema_consistency(initial_files[0])

        # Step 2: Append new data
        data2 = self.create_sample_data(start_date='2024-01-01 01:40:00', num_rows=50)
        helper.save_price_parquet(
            data2,
            self.dest_dir,
            append=True,
            n_partitions=None
        )

        appended_files = glob.glob(f"{self.dest_dir}/part.*.parquet")
        print(f"After append: {len(appended_files)} files")

        # Get schema after append
        appended_schema_fields = self.check_schema_consistency(appended_files[0])

        # Verify schema consistency
        self.assertEqual(
            set(initial_schema_fields),
            set(appended_schema_fields),
            "Schema should remain consistent after append"
        )

        # Read back and verify
        df_read = self.read_parquet_files(self.dest_dir)
        self.assertEqual(len(df_read), 150, "Should have 100 + 50 = 150 rows")

        # Check for duplicates
        duplicates = df_read.index.duplicated().sum()
        self.assertEqual(duplicates, 0, "Should not have duplicate dates")

        # Verify data is sorted
        self.assertTrue(df_read.index.is_monotonic_increasing, "Index should be sorted")

        print(f"✓ Append successful: {len(df_read)} total rows")
        print(f"✓ No duplicates: {duplicates} duplicates found")
        print(f"✓ Schema consistent between writes")

    def test_03_multiple_appends(self):
        """Test multiple sequential appends"""
        print("\n" + "="*60)
        print("TEST 3: Multiple Appends")
        print("="*60)

        # Initial write
        data1 = self.create_sample_data(start_date='2024-01-01 00:00:00', num_rows=50)
        helper.save_price_parquet(data1, self.dest_dir, append=False, overwrite=True)

        expected_rows = 50

        # Multiple appends
        for i in range(3):
            start_time = datetime(2024, 1, 1, 0, 50, 0) + timedelta(minutes=i * 50)
            data = self.create_sample_data(start_date=start_time, num_rows=50)
            helper.save_price_parquet(data, self.dest_dir, append=True)
            expected_rows += 50
            print(f"Append {i+1}: Added 50 rows")

        # Verify final result
        df_read = self.read_parquet_files(self.dest_dir)
        self.assertEqual(len(df_read), expected_rows, f"Should have {expected_rows} rows")

        # Check all schemas are consistent
        import glob
        parquet_files = glob.glob(f"{self.dest_dir}/part.*.parquet")
        schemas = []
        for pf in parquet_files:
            s = pq.read_schema(pf)
            schemas.append({field.name for field in s})

        # All schemas should be identical
        first_schema = schemas[0]
        for s in schemas[1:]:
            self.assertEqual(first_schema, s, "All parquet files should have identical schema")

        print(f"✓ Multiple appends successful: {len(df_read)} total rows")
        print(f"✓ All {len(parquet_files)} files have consistent schema")

    def test_04_overwrite_existing(self):
        """Test overwriting existing data"""
        print("\n" + "="*60)
        print("TEST 4: Overwrite Existing Data")
        print("="*60)

        # Initial write
        data1 = self.create_sample_data(start_date='2024-01-01 00:00:00', num_rows=100)
        helper.save_price_parquet(data1, self.dest_dir, append=False, overwrite=True)

        df_initial = self.read_parquet_files(self.dest_dir)
        initial_count = len(df_initial)
        print(f"Initial data: {initial_count} rows")

        data2 = self.create_sample_data(start_date='2024-02-01 00:00:00', num_rows=200)
        helper.save_price_parquet(data2, self.dest_dir, append=False, overwrite=True)

        df_overwritten = self.read_parquet_files(self.dest_dir)
        overwritten_count = len(df_overwritten)
        print(f"After overwrite: {overwritten_count} rows")

        # Verify old data is gone
        self.assertEqual(overwritten_count, 200, "Should have only new data")
        self.assertNotEqual(
            df_initial.index.min(),
            df_overwritten.index.min(),
            "Data should be completely replaced"
        )

        print(f"✓ Overwrite successful: old data replaced")

    def test_05_partition_isolation(self):
        """Test that different symbols don't interfere with each other"""
        print("\n" + "="*60)
        print("TEST 5: Partition Isolation (Multiple Symbols)")
        print("="*60)

        # Write data for symbol 1
        symbol1 = "BTCUSDT"
        dest_dir1 = f"{self.test_root}/exchange={self.exchange}/symbol={symbol1}"
        data1 = self.create_sample_data(start_date='2024-01-01 00:00:00', num_rows=100)
        data1['symbol'] = symbol1

        helper.save_price_parquet(data1, dest_dir1, append=False, overwrite=True)

        # Write data for symbol 2
        symbol2 = "ETHUSDT"
        dest_dir2 = f"{self.test_root}/exchange={self.exchange}/symbol={symbol2}"
        data2 = self.create_sample_data(start_date='2024-01-01 00:00:00', num_rows=150)
        data2['symbol'] = symbol2

        helper.save_price_parquet(data2, dest_dir2, append=False, overwrite=True)

        df1 = self.read_parquet_files(dest_dir1)
        df2 = self.read_parquet_files(dest_dir2)

        self.assertEqual(len(df1), 100, "Symbol 1 should have 100 rows")
        self.assertEqual(len(df2), 150, "Symbol 2 should have 150 rows")

        # Overwrite symbol 1
        data1_new = self.create_sample_data(start_date='2024-02-01 00:00:00', num_rows=50)
        data1_new['symbol'] = symbol1

        helper.save_price_parquet(data1_new, dest_dir1, append=False, overwrite=True)

        # Verify symbol 1 changed but symbol 2 unchanged
        df1_new = self.read_parquet_files(dest_dir1)
        df2_check = self.read_parquet_files(dest_dir2)

        self.assertEqual(len(df1_new), 50, "Symbol 1 should have new data (50 rows)")
        self.assertEqual(len(df2_check), 150, "Symbol 2 should be unchanged (150 rows)")

        print(f"✓ Partition isolation verified")
        print(f"  - {symbol1}: {len(df1_new)} rows")
        print(f"  - {symbol2}: {len(df2_check)} rows (unchanged)")


if __name__ == '__main__':
    unittest.main(verbosity=2)
