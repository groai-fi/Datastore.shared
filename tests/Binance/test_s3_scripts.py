"""
Tests for S3 CLI scripts:
  - s3_utils core helpers (mocked — runs in CI)
  - download_price_binance_s3 (integration — requires live S3)
  - merge_parquet_prices_s3   (integration — requires live S3)
  - list_symbols_s3           (integration — requires live S3)
  - remove_symbol_s3          (integration — requires live S3)

Marker strategy
---------------
No marker        → runs in every CI job; uses unittest.mock to avoid boto3/duckdb.
@pytest.mark.integration → requires live S3 credentials in environment:
    S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
    BINANCE_API_KEY, BINANCE_API_SECRET

Run integration tests locally:
    pytest -m integration tests/Binance/test_s3_scripts.py
"""
import os
import sys
import shutil
import tempfile
import unittest
from io import StringIO
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import pandas as pd
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests — mocked; run in CI
# ─────────────────────────────────────────────────────────────────────────────

class TestS3UtilsPaths(unittest.TestCase):
    """Test path-construction helpers (no I/O, no mocking needed)."""

    def test_get_s3_prefix(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import get_s3_prefix
        result = get_s3_prefix("my-bucket", "prices_v3.parquet", "Binance", "BTCUSDT")
        self.assertEqual(
            result,
            "s3://my-bucket/prices_v3.parquet/exchange=Binance/symbol=BTCUSDT"
        )

    def test_get_s3_glob(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import get_s3_glob
        result = get_s3_glob("my-bucket", "prices_v3.parquet", "Binance", "BTCUSDT")
        self.assertEqual(
            result,
            "s3://my-bucket/prices_v3.parquet/exchange=Binance/symbol=BTCUSDT/part.*.parquet"
        )

    def test_get_s3_prefix_strips_nothing(self):
        """Prefix should not contain trailing slash."""
        from groai_fi_datastore_shared.Binance.cli.s3_utils import get_s3_prefix
        result = get_s3_prefix("bkt", "root", "Ex", "SYM")
        self.assertFalse(result.endswith("/"))


class TestDeleteS3KeysMocked(unittest.TestCase):
    """Test delete_s3_keys with boto3 mocked out."""

    def _make_mock_s3(self, error_keys=None):
        """Return a mock boto3 client that simulates delete_objects."""
        error_keys = error_keys or []
        mock_s3 = MagicMock()
        mock_s3.delete_objects.return_value = {
            "Errors": [{"Key": k, "Message": "simulated error"} for k in error_keys]
        }
        return mock_s3

    def test_empty_key_list_returns_zero(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import delete_s3_keys
        with patch("groai_fi_datastore_shared.Binance.cli.s3_utils._boto3_client") as mk:
            result = delete_s3_keys("bkt", [])
        self.assertEqual(result, 0)
        mk.assert_not_called()

    def test_single_batch_all_succeed(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import delete_s3_keys
        keys = ["prefix/part.1.parquet", "prefix/part.2.parquet"]
        with patch("groai_fi_datastore_shared.Binance.cli.s3_utils._boto3_client") as mk:
            mk.return_value = self._make_mock_s3()
            result = delete_s3_keys("bkt", keys)
        self.assertEqual(result, 2)

    def test_partial_failures_counted_correctly(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import delete_s3_keys
        keys = ["prefix/part.1.parquet", "prefix/part.2.parquet", "prefix/part.3.parquet"]
        # Simulate one key failing
        with patch("groai_fi_datastore_shared.Binance.cli.s3_utils._boto3_client") as mk:
            mk.return_value = self._make_mock_s3(error_keys=["prefix/part.2.parquet"])
            result = delete_s3_keys("bkt", keys)
        self.assertEqual(result, 2)  # 3 attempted - 1 error = 2 deleted

    def test_large_batch_split_into_chunks_of_1000(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import delete_s3_keys
        keys = [f"prefix/part.{i}.parquet" for i in range(2500)]
        with patch("groai_fi_datastore_shared.Binance.cli.s3_utils._boto3_client") as mk:
            mock_s3 = self._make_mock_s3()
            mk.return_value = mock_s3
            result = delete_s3_keys("bkt", keys)

        # 2500 keys → ceil(2500/1000) = 3 batches
        self.assertEqual(mock_s3.delete_objects.call_count, 3)
        self.assertEqual(result, 2500)


class TestListS3SymbolsMocked(unittest.TestCase):
    """Test list_s3_symbols with boto3 paginator mocked."""

    def test_returns_sorted_symbol_names(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import list_s3_symbols

        fake_page = {
            "CommonPrefixes": [
                {"Prefix": "prices/exchange=Binance/symbol=SOLUSDT/"},
                {"Prefix": "prices/exchange=Binance/symbol=BTCUSDT/"},
                {"Prefix": "prices/exchange=Binance/symbol=ETHUSDT/"},
            ]
        }
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [fake_page]

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        with patch("groai_fi_datastore_shared.Binance.cli.s3_utils._boto3_client",
                   return_value=mock_s3):
            symbols = list_s3_symbols("bkt", "prices", "Binance")

        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT", "SOLUSDT"])

    def test_ignores_non_symbol_prefixes(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import list_s3_symbols

        fake_page = {
            "CommonPrefixes": [
                {"Prefix": "prices/exchange=Binance/symbol=BTCUSDT/"},
                {"Prefix": "prices/exchange=Binance/_metadata/"},    # not a symbol
            ]
        }
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [fake_page]

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        with patch("groai_fi_datastore_shared.Binance.cli.s3_utils._boto3_client",
                   return_value=mock_s3):
            symbols = list_s3_symbols("bkt", "prices", "Binance")

        self.assertEqual(symbols, ["BTCUSDT"])

    def test_empty_bucket_returns_empty_list(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import list_s3_symbols

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"CommonPrefixes": []}]

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        with patch("groai_fi_datastore_shared.Binance.cli.s3_utils._boto3_client",
                   return_value=mock_s3):
            symbols = list_s3_symbols("bkt", "prices", "Binance")

        self.assertEqual(symbols, [])


class TestCountPartsMocked(unittest.TestCase):
    """Test count_parts_s3 with boto3 paginator mocked."""

    def test_counts_only_parquet_parts(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import count_parts_s3

        fake_page = {
            "Contents": [
                {"Key": "prices/exchange=Binance/symbol=BTCUSDT/part.00000.parquet"},
                {"Key": "prices/exchange=Binance/symbol=BTCUSDT/part.1700000000.parquet"},
                {"Key": "prices/exchange=Binance/symbol=BTCUSDT/.write.lock"},  # not a part
                {"Key": "prices/exchange=Binance/symbol=BTCUSDT/part.1700000001.parquet"},
            ]
        }
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [fake_page]

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        with patch("groai_fi_datastore_shared.Binance.cli.s3_utils._boto3_client",
                   return_value=mock_s3):
            count = count_parts_s3("bkt", "prices", "Binance", "BTCUSDT")

        self.assertEqual(count, 3)  # 3 part.*.parquet files (excluding .write.lock)


class TestListPartKeysMocked(unittest.TestCase):
    """Test list_part_keys with boto3 paginator mocked."""

    def test_excludes_specified_filename(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import list_part_keys

        fake_page = {
            "Contents": [
                {"Key": "prices/exchange=Binance/symbol=BTCUSDT/part.00000.parquet"},
                {"Key": "prices/exchange=Binance/symbol=BTCUSDT/part.1700000000.parquet"},
            ]
        }
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [fake_page]

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        with patch("groai_fi_datastore_shared.Binance.cli.s3_utils._boto3_client",
                   return_value=mock_s3):
            keys = list_part_keys("bkt", "prices", "Binance", "BTCUSDT",
                                  exclude="part.00000.parquet")

        self.assertEqual(len(keys), 1)
        self.assertIn("part.1700000000.parquet", keys[0])
        self.assertNotIn("part.00000.parquet", keys[0])


class TestRemoveSymbolS3Mocked(unittest.TestCase):
    """Test remove_symbol_s3 CLI with mocked boto3."""

    def _argv(self, symbol, extra=None):
        base = ["binance-remove-symbol-s3", "--symbol", symbol,
                "--bucket", "test-bucket", "--yes"]
        return base + (extra or [])

    def _mock_s3_with_keys(self, keys):
        """Return a mock boto3 client with paginator listing given keys."""
        fake_page = {"Contents": [{"Key": k} for k in keys]}
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [fake_page]
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_s3.delete_objects.return_value = {"Errors": []}
        return mock_s3

    def test_no_keys_found_exits_zero(self):
        from groai_fi_datastore_shared.Binance.cli.remove_symbol_s3 import run

        mock_s3 = self._mock_s3_with_keys([])  # nothing found
        with patch("groai_fi_datastore_shared.Binance.cli.remove_symbol_s3._boto3_client",
                   return_value=mock_s3):
            with patch("sys.argv", self._argv("GHOST")):
                with self.assertRaises(SystemExit) as ctx:
                    run()
                self.assertEqual(ctx.exception.code, 0)

    def test_with_keys_calls_delete(self):
        from groai_fi_datastore_shared.Binance.cli.remove_symbol_s3 import run

        keys = ["prices/exchange=Binance/symbol=BTCUSDT/part.00000.parquet"]
        mock_s3 = self._mock_s3_with_keys(keys)

        with patch("groai_fi_datastore_shared.Binance.cli.remove_symbol_s3._boto3_client",
                   return_value=mock_s3):
            with patch("groai_fi_datastore_shared.Binance.cli.s3_utils._boto3_client",
                       return_value=mock_s3):
                with patch("sys.argv", self._argv("BTCUSDT")):
                    run()

        mock_s3.delete_objects.assert_called_once()

    def test_abort_on_wrong_confirmation(self):
        """Without --yes and wrong confirmation answer, nothing should be deleted."""
        from groai_fi_datastore_shared.Binance.cli.remove_symbol_s3 import run

        keys = ["prices/exchange=Binance/symbol=BTCUSDT/part.00000.parquet"]
        mock_s3 = self._mock_s3_with_keys(keys)

        with patch("groai_fi_datastore_shared.Binance.cli.remove_symbol_s3._boto3_client",
                   return_value=mock_s3):
            with patch("builtins.input", return_value="WRONG"):
                with patch("sys.argv", ["binance-remove-symbol-s3",
                                        "--symbol", "BTCUSDT",
                                        "--bucket", "test-bucket"]):
                    with self.assertRaises(SystemExit) as ctx:
                        run()
                    self.assertEqual(ctx.exception.code, 0)

        mock_s3.delete_objects.assert_not_called()


class TestListSymbolsS3Mocked(unittest.TestCase):
    """Test list_symbols_s3 with mocked boto3 + duckdb.

    Skipped automatically when duckdb is not installed in the environment.
    """

    @classmethod
    def setUpClass(cls):
        pytest.importorskip("duckdb", reason="duckdb not installed — skip list_symbols_s3 mocked tests")

    def test_no_symbols_exits_zero(self):
        from groai_fi_datastore_shared.Binance.cli.list_symbols_s3 import run

        # No symbols on S3
        with patch("groai_fi_datastore_shared.Binance.cli.list_symbols_s3.list_s3_symbols",
                   return_value=[]):
            with patch("sys.argv", ["binance-list-symbols-s3",
                                    "--bucket", "test-bucket"]):
                with self.assertRaises(SystemExit) as ctx:
                    run()
                self.assertEqual(ctx.exception.code, 0)

    def test_symbols_printed(self):
        from groai_fi_datastore_shared.Binance.cli.list_symbols_s3 import run

        captured = StringIO()
        with patch("groai_fi_datastore_shared.Binance.cli.list_symbols_s3.list_s3_symbols",
                   return_value=["BTCUSDT", "ETHUSDT"]):
            with patch("groai_fi_datastore_shared.Binance.cli.list_symbols_s3.get_max_date_s3",
                       return_value=datetime(2024, 4, 10, tzinfo=timezone.utc)):
                with patch("groai_fi_datastore_shared.Binance.cli.list_symbols_s3.count_parts_s3",
                           return_value=1):
                    with patch("groai_fi_datastore_shared.Binance.cli.list_symbols_s3.configure_duckdb_s3",
                               return_value=MagicMock()):
                        with patch("sys.argv", ["binance-list-symbols-s3",
                                                "--bucket", "test-bucket"]):
                            with patch("sys.stdout", captured):
                                run()

        output = captured.getvalue()
        self.assertIn("BTCUSDT", output)
        self.assertIn("ETHUSDT", output)

    def test_missing_bucket_exits_nonzero(self):
        from groai_fi_datastore_shared.Binance.cli.list_symbols_s3 import run

        with patch.dict(os.environ, {}, clear=True):
            # Ensure S3_BUCKET_NAME is not set
            env = {k: v for k, v in os.environ.items() if k != "S3_BUCKET_NAME"}
            with patch.dict(os.environ, env, clear=True):
                with patch("sys.argv", ["binance-list-symbols-s3"]):
                    with self.assertRaises(SystemExit) as ctx:
                        run()
                    self.assertEqual(ctx.exception.code, 1)


# ─────────────────────────────────────────────────────────────────────────────
# Schema contract unit tests — pure PyArrow, no S3 connection needed
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaContractUnit(unittest.TestCase):
    """
    Verify that _shape_df + the write_parquet_to_s3 PyArrow logic produce a
    price_parquet_v3-compliant schema.

    No S3 connection is required — writes are done to a local temp file.
    """

    def _make_raw_klines(self, n: int = 120) -> pd.DataFrame:
        """Simulate raw klines as returned by download_data_from_binance_1minute."""
        dates = pd.date_range("2024-01-01", periods=n, freq="1min", tz="UTC")
        return pd.DataFrame({
            "date":     (dates.astype("int64") // 10 ** 6).tolist(),  # ms integers
            "open":     100.0, "high": 101.0, "low": 99.0,
            "close":    100.5, "volume": 1000.0,
            # Binance klines have 12 columns; _shape_df slices the first 6
            "close_time": 0, "qav": 0.0, "num_trades": 0,
            "tbb": 0.0, "tbq": 0.0, "ignore": 0.0,
        })

    # ------------------------------------------------------------------
    # Rule 1 — date as index
    # ------------------------------------------------------------------

    def test_shape_df_date_is_index(self):
        """_shape_df must return a DataFrame with 'date' as the named index."""
        from groai_fi_datastore_shared.Binance.cli.download_price_binance_s3 import _shape_df

        shaped = _shape_df(self._make_raw_klines(), symbol="BTCUSDT", exchange="Binance")

        self.assertEqual(shaped.index.name, "date",
                         "index must be named 'date'")
        self.assertNotIn("date", shaped.columns,
                         "'date' must not also appear as a regular column")

    def test_shape_df_date_index_is_utc(self):
        """The date index must be a UTC-aware DatetimeTZDtype."""
        from groai_fi_datastore_shared.Binance.cli.download_price_binance_s3 import _shape_df
        import pyarrow as pa

        shaped = _shape_df(self._make_raw_klines(), symbol="BTCUSDT", exchange="Binance")

        # Inspect via PyArrow table to check the actual Parquet-level type
        table = pa.Table.from_pandas(shaped, preserve_index=True)
        date_type = table.schema.field("date").type
        self.assertTrue(pa.types.is_timestamp(date_type),
                        f"date must be a timestamp type, got {date_type}")
        self.assertEqual(str(date_type.tz), "UTC",
                         f"date must be UTC-tagged, got tz={date_type.tz}")

    # ------------------------------------------------------------------
    # Rule 3 — exchange / symbol not in file data
    # ------------------------------------------------------------------

    def test_shape_df_no_hive_columns(self):
        """exchange and symbol must not appear as data columns."""
        from groai_fi_datastore_shared.Binance.cli.download_price_binance_s3 import _shape_df

        shaped = _shape_df(self._make_raw_klines(), symbol="BTCUSDT", exchange="Binance")

        self.assertNotIn("exchange", shaped.columns,
                         "'exchange' must not be a file data column")
        self.assertNotIn("symbol", shaped.columns,
                         "'symbol' must not be a file data column")

    # ------------------------------------------------------------------
    # Rule 2 — yymm dict-encoded  |  Rule 3 — no exchange/symbol in schema
    # Rule 1 — date is Parquet index  |  Rule 4 — UTC timezone
    # (full spec validation via a local temp Parquet file)
    # ------------------------------------------------------------------

    def test_write_path_produces_spec_compliant_schema(self):
        """
        Replicate the write_parquet_to_s3 encoding logic and write to a local
        temp file, then validate every rule from PRICE_PARQUET_V3_SPEC.md.
        """
        import tempfile
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
        from groai_fi_datastore_shared.Binance.cli.download_price_binance_s3 import _shape_df

        shaped = _shape_df(self._make_raw_klines(), symbol="BTCUSDT", exchange="Binance")

        # ── Replicate write_parquet_to_s3 encoding (without the S3 transport) ─
        working_df = shaped.copy()
        if "yymm" in working_df.columns:
            working_df["yymm"] = working_df["yymm"].astype(str)

        table = pa.Table.from_pandas(working_df, preserve_index=True)

        if "yymm" in table.schema.names:
            idx = table.schema.get_field_index("yymm")
            yymm_utf8 = table.column("yymm").cast(pa.utf8())
            encoded   = pc.dictionary_encode(yymm_utf8)
            table     = table.set_column(idx, pa.field("yymm", encoded.type), encoded)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as fh:
            tmp_path = fh.name
        try:
            pq.write_table(table, tmp_path, compression="snappy")
            schema    = pq.read_schema(tmp_path)
            field_names = [f.name for f in schema]

            # Rule 3: exchange and symbol NOT in file schema
            self.assertNotIn("exchange", field_names,
                             "exchange must not be a file column (Hive path only)")
            self.assertNotIn("symbol", field_names,
                             "symbol must not be a file column (Hive path only)")

            # Rule 2: yymm is dictionary-encoded
            self.assertIn("yymm", field_names, "yymm must be present")
            yymm_type = schema.field("yymm").type
            self.assertTrue(pa.types.is_dictionary(yymm_type),
                            f"yymm must be dictionary-encoded, got {yymm_type}")

            # Rule 1 + Rule 4: date is Parquet-level index, UTC timestamp
            # When preserve_index=True, 'date' appears in schema.names as the stored field
            self.assertIn("date", field_names,
                          "date must be stored as a Parquet field (as the index)")
            date_type = schema.field("date").type
            self.assertTrue(pa.types.is_timestamp(date_type),
                            f"date must be a timestamp type, got {date_type}")
            self.assertEqual(str(date_type.tz), "UTC",
                             f"date must be UTC-tagged, got tz={date_type.tz}")

            # Verify pandas metadata marks date as index (not data column)
            pandas_meta   = schema.pandas_metadata
            index_columns = pandas_meta.get("index_columns", [])
            self.assertIn("date", index_columns,
                          "pandas metadata must mark 'date' as the index column")
        finally:
            import os as _os
            _os.unlink(tmp_path)


# ─────────────────────────────────────────────────────────────────────────────
# Integration tests — require live S3 + Binance API
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
class TestDownloadPriceBinanceS3(unittest.TestCase):
    """
    Integration test for download_price_binance_s3.

    Requires environment variables:
        S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
        BINANCE_API_KEY, BINANCE_API_SECRET

    Downloads a small slice of recent BCHUSDT data and verifies the parquet
    lands on S3, then cleans up the test prefix.

    Run with: pytest -m integration
    """

    SYMBOL     = "BCHUSDT"
    EXCHANGE   = "Binance"
    PRICE_ROOT = "prices_v3_test.parquet"   # separate root to avoid polluting real data

    @classmethod
    def setUpClass(cls):
        cls.bucket = os.environ.get("S3_BUCKET_NAME", "")
        if not cls.bucket:
            pytest.skip("S3_BUCKET_NAME not set — skipping S3 integration tests")

    def tearDown(self):
        """Clean up test prefix after each test."""
        from groai_fi_datastore_shared.Binance.cli.s3_utils import delete_s3_prefix
        prefix = f"{self.PRICE_ROOT}/exchange={self.EXCHANGE}/symbol={self.SYMBOL}/"
        try:
            delete_s3_prefix(self.bucket, prefix)
        except Exception:
            pass  # best-effort cleanup

    def test_download_creates_part_file(self):
        """Fresh download should create at least one part file on S3."""
        import duckdb
        from groai_fi_datastore_shared.Binance.cli.download_price_binance_s3 import run_for_symbol
        from groai_fi_datastore_shared.Binance.cli.s3_utils import (
            configure_duckdb_s3, count_parts_s3, get_s3_glob,
        )

        start = datetime.now(tz=timezone.utc) - timedelta(days=2)
        rows = run_for_symbol(
            symbol=self.SYMBOL,
            tframe="1m",
            bucket=self.bucket,
            price_root=self.PRICE_ROOT,
            start_date_fallback=start,
            exchange=self.EXCHANGE,
        )

        self.assertGreater(rows, 0, "Should have downloaded at least 1 row")

        part_count = count_parts_s3(self.bucket, self.PRICE_ROOT, self.EXCHANGE, self.SYMBOL)
        self.assertGreaterEqual(part_count, 1, "Should have at least 1 part file on S3")

        print(f"✓ Downloaded {rows} rows into {part_count} part file(s)")

    def test_idempotent_download_does_not_duplicate(self):
        """Running download twice should not duplicate rows."""
        import duckdb
        from groai_fi_datastore_shared.Binance.cli.download_price_binance_s3 import run_for_symbol
        from groai_fi_datastore_shared.Binance.cli.s3_utils import (
            configure_duckdb_s3, count_rows_s3, get_s3_glob,
        )

        start = datetime.now(tz=timezone.utc) - timedelta(days=2)

        run_for_symbol(
            symbol=self.SYMBOL, tframe="1m", bucket=self.bucket,
            price_root=self.PRICE_ROOT, start_date_fallback=start,
            exchange=self.EXCHANGE,
        )

        con  = configure_duckdb_s3(duckdb.connect())
        glob = get_s3_glob(self.bucket, self.PRICE_ROOT, self.EXCHANGE, self.SYMBOL)
        rows_after_first = count_rows_s3(con, glob)

        # Second call — should return 0 new rows (already up to date)
        rows2 = run_for_symbol(
            symbol=self.SYMBOL, tframe="1m", bucket=self.bucket,
            price_root=self.PRICE_ROOT, start_date_fallback=start,
            exchange=self.EXCHANGE,
        )
        self.assertEqual(rows2, 0, "Should download 0 rows if already up to date")

        rows_after_second = count_rows_s3(con, glob)
        self.assertEqual(rows_after_first, rows_after_second,
                         "Row count should not change after redundant download")

        print(f"✓ Idempotent: {rows_after_first} rows before == {rows_after_second} rows after")


@pytest.mark.integration
class TestMergeParquetS3(unittest.TestCase):
    """
    Integration test for merge_parquet_prices_s3.

    Uploads two synthetic part files to S3, runs the merge, then validates
    the result and cleans up.
    """

    SYMBOL     = "TESTUSDT"   # synthetic symbol — never touches real data
    EXCHANGE   = "Binance"
    PRICE_ROOT = "prices_v3_test.parquet"

    @classmethod
    def setUpClass(cls):
        cls.bucket = os.environ.get("S3_BUCKET_NAME", "")
        if not cls.bucket:
            pytest.skip("S3_BUCKET_NAME not set — skipping S3 integration tests")

    def _upload_synthetic_parts(self, n_parts: int = 3):
        """Upload n spec-compliant synthetic part files and return total row count."""
        from groai_fi_datastore_shared.Binance.cli.s3_utils import (
            get_s3_prefix, write_parquet_to_s3,
        )
        total_rows = 0

        for i in range(n_parts):
            base_ts = 1_700_000_000 + i * 3600
            dates   = pd.date_range(
                start=datetime.fromtimestamp(base_ts, tz=timezone.utc),
                periods=60, freq="1min"
            )
            # Build spec-compliant DataFrame: date as index, no exchange/symbol columns
            df = pd.DataFrame({
                "yymm":   dates.strftime("%y%m"),
                "open":   100.0, "high": 101.0, "low": 99.0,
                "close":  100.5, "volume": 1000.0,
            }, index=pd.Index(dates, name="date"))

            prefix = get_s3_prefix(self.bucket, self.PRICE_ROOT, self.EXCHANGE, self.SYMBOL)
            dest   = f"{prefix}/part.{base_ts}.parquet"
            write_parquet_to_s3(df, dest)
            total_rows += len(df)
            print(f"  Uploaded part {i+1}: {len(df)} rows → {dest}")

        return total_rows

    def tearDown(self):
        from groai_fi_datastore_shared.Binance.cli.s3_utils import delete_s3_prefix
        prefix = f"{self.PRICE_ROOT}/exchange={self.EXCHANGE}/symbol={self.SYMBOL}/"
        try:
            delete_s3_prefix(self.bucket, prefix)
        except Exception:
            pass

    def test_merge_consolidates_parts_and_validates_row_count(self):
        """Merge should produce one part.00000.parquet and preserve every row."""
        import duckdb
        from groai_fi_datastore_shared.Binance.cli.merge_parquet_prices_s3 import run_for_symbol
        from groai_fi_datastore_shared.Binance.cli.s3_utils import (
            configure_duckdb_s3, count_rows_s3, count_parts_s3, get_s3_glob,
        )

        expected_rows = self._upload_synthetic_parts(n_parts=3)
        print(f"Pre-merge: {expected_rows} rows in 3 parts")

        success = run_for_symbol(
            symbol=self.SYMBOL,
            bucket=self.bucket,
            price_root=self.PRICE_ROOT,
            exchange=self.EXCHANGE,
            no_delete_parts=False,
        )
        self.assertTrue(success, "Merge should succeed")

        # After merge: exactly 1 part file
        part_count = count_parts_s3(self.bucket, self.PRICE_ROOT, self.EXCHANGE, self.SYMBOL)
        self.assertEqual(part_count, 1, "Merge should leave exactly 1 part file")

        # Row count must be preserved (deduplication may reduce slightly if synthetic data overlaps)
        con  = configure_duckdb_s3(duckdb.connect())
        glob = get_s3_glob(self.bucket, self.PRICE_ROOT, self.EXCHANGE, self.SYMBOL)
        merged_rows = count_rows_s3(con, glob)
        self.assertEqual(merged_rows, expected_rows,
                         f"Merged row count {merged_rows} should equal pre-merge {expected_rows}")

        print(f"✓ Merged: {merged_rows} rows in 1 file")

    def test_merge_raises_on_validation_failure(self):
        """If row counts mismatch, merge should raise RuntimeError and leave old parts intact."""
        import duckdb
        from groai_fi_datastore_shared.Binance.cli.s3_utils import (
            configure_duckdb_s3, count_rows_s3,
        )

        self._upload_synthetic_parts(n_parts=2)

        # Patch count_rows_s3 to return a wrong post-merge count, simulating corruption
        with patch("groai_fi_datastore_shared.Binance.cli.merge_parquet_prices_s3.count_rows_s3",
                   side_effect=[120, 119]):   # pre=120, post=119 (mismatch)
            from groai_fi_datastore_shared.Binance.cli.merge_parquet_prices_s3 import run_for_symbol
            with self.assertRaises(RuntimeError) as ctx:
                run_for_symbol(
                    symbol=self.SYMBOL,
                    bucket=self.bucket,
                    price_root=self.PRICE_ROOT,
                    exchange=self.EXCHANGE,
                )
            self.assertIn("validation FAILED", str(ctx.exception))
            print(f"✓ RuntimeError raised as expected: {ctx.exception}")


@pytest.mark.integration
class TestListSymbolsS3Integration(unittest.TestCase):
    """Integration test for binance-list-symbols-s3 against a live bucket."""

    PRICE_ROOT = "prices_v3_test.parquet"
    EXCHANGE   = "Binance"

    @classmethod
    def setUpClass(cls):
        cls.bucket = os.environ.get("S3_BUCKET_NAME", "")
        if not cls.bucket:
            pytest.skip("S3_BUCKET_NAME not set — skipping S3 integration tests")

    def test_list_after_upload_shows_symbol(self):
        """After uploading a synthetic part, the symbol should appear in the listing."""
        import duckdb
        from groai_fi_datastore_shared.Binance.cli.s3_utils import (
            configure_duckdb_s3, get_s3_prefix, list_s3_symbols, delete_s3_prefix,
        )

        symbol = "LISTTEST"
        try:
            con    = configure_duckdb_s3(duckdb.connect())
            prefix = get_s3_prefix(self.bucket, self.PRICE_ROOT, self.EXCHANGE, symbol)
            dates  = pd.date_range("2024-01-01", periods=10, freq="1min", tz="UTC")
            df     = pd.DataFrame({"date": dates, "close": 100.0})
            dest   = f"{prefix}/part.00000.parquet"
            con.register("df_test", df)
            con.execute(f"COPY df_test TO '{dest}' (FORMAT PARQUET, COMPRESSION SNAPPY)")

            symbols = list_s3_symbols(self.bucket, self.PRICE_ROOT, self.EXCHANGE)
            self.assertIn(symbol, symbols,
                          f"Uploaded symbol '{symbol}' should appear in list_s3_symbols result")
            print(f"✓ Found {symbol} in listing: {symbols}")
        finally:
            prefix_str = f"{self.PRICE_ROOT}/exchange={self.EXCHANGE}/symbol={symbol}/"
            try:
                delete_s3_prefix(self.bucket, prefix_str)
            except Exception:
                pass


@pytest.mark.integration
class TestRemoveSymbolS3Integration(unittest.TestCase):
    """Integration test for binance-remove-symbol-s3 against a live bucket."""

    PRICE_ROOT = "prices_v3_test.parquet"
    EXCHANGE   = "Binance"

    @classmethod
    def setUpClass(cls):
        cls.bucket = os.environ.get("S3_BUCKET_NAME", "")
        if not cls.bucket:
            pytest.skip("S3_BUCKET_NAME not set — skipping S3 integration tests")

    def test_remove_deletes_prefix(self):
        """After removal, the symbol should not appear in listing."""
        import duckdb
        from groai_fi_datastore_shared.Binance.cli.s3_utils import (
            configure_duckdb_s3, get_s3_prefix, list_s3_symbols, delete_s3_prefix,
        )
        from groai_fi_datastore_shared.Binance.cli.remove_symbol_s3 import run

        symbol = "REMOVETEST"
        try:
            con    = configure_duckdb_s3(duckdb.connect())
            prefix = get_s3_prefix(self.bucket, self.PRICE_ROOT, self.EXCHANGE, symbol)
            dates  = pd.date_range("2024-01-01", periods=10, freq="1min", tz="UTC")
            df     = pd.DataFrame({"date": dates, "close": 100.0})
            dest   = f"{prefix}/part.00000.parquet"
            con.register("df_rm", df)
            con.execute(f"COPY df_rm TO '{dest}' (FORMAT PARQUET, COMPRESSION SNAPPY)")

            # Verify it's there before removing
            symbols_before = list_s3_symbols(self.bucket, self.PRICE_ROOT, self.EXCHANGE)
            self.assertIn(symbol, symbols_before, "Symbol should be present before removal")

            with patch("sys.argv", ["binance-remove-symbol-s3",
                                    "--symbol", symbol,
                                    "--bucket", self.bucket,
                                    "--price-root", self.PRICE_ROOT,
                                    "--yes"]):
                run()

            symbols_after = list_s3_symbols(self.bucket, self.PRICE_ROOT, self.EXCHANGE)
            self.assertNotIn(symbol, symbols_after,
                             "Symbol should NOT be present after removal")
            print(f"✓ {symbol} removed from S3 listing")
        finally:
            prefix_str = f"{self.PRICE_ROOT}/exchange={self.EXCHANGE}/symbol={symbol}/"
            try:
                delete_s3_prefix(self.bucket, prefix_str)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
