"""
Tests for local list_symbols and remove_symbol CLI scripts.

All tests here are pure unit tests — they operate on temporary directories
with synthetic parquet files and require NO live API connections.
These tests run in CI without any special markers.
"""
import os
import sys
import shutil
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timedelta
from io import StringIO
from unittest.mock import patch

import pandas as pd
import pytest


class TestListSymbols(unittest.TestCase):
    """Test binance-list-symbols (local filesystem) behaviour."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix="groai_test_list_symbols_")
        self.price_root = Path(self._tmpdir) / "prices_v3.parquet"
        self.exchange   = "Binance"
        self.exchange_dir = self.price_root / f"exchange={self.exchange}"

    def tearDown(self):
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    # ── helpers ──────────────────────────────────────────────────────────────

    def _make_symbol(self, symbol: str, num_parts: int = 1, last_date: str = "2024-01-10"):
        """Write synthetic parquet file(s) for a symbol."""
        symbol_dir = self.exchange_dir / f"symbol={symbol}"
        symbol_dir.mkdir(parents=True, exist_ok=True)

        base_dt = datetime.fromisoformat(last_date)
        for i in range(num_parts):
            # Each part file has 60 rows of 1-min candles ending at base_dt
            dates = pd.date_range(end=base_dt - timedelta(minutes=i * 60),
                                  periods=60, freq="1min")
            df = pd.DataFrame({
                "date": dates,
                "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
                "volume": 1000.0,
            })
            fname = "part.00000.parquet" if (num_parts == 1 and i == 0) \
                else f"part.{1_700_000_000 + i}.parquet"
            df.to_parquet(symbol_dir / fname, engine="pyarrow", index=False)

        return symbol_dir

    # ── tests ─────────────────────────────────────────────────────────────────

    def test_no_symbols_shows_message(self):
        """Empty exchange dir should show 'no symbols' message."""
        self.exchange_dir.mkdir(parents=True, exist_ok=True)

        from groai_fi_datastore_shared.Binance.cli.list_symbols import run

        with patch("sys.argv", ["binance-list-symbols",
                                "--path", str(self.price_root),
                                "--exchange", self.exchange]):
            with self.assertRaises(SystemExit) as ctx:
                run()
            self.assertEqual(ctx.exception.code, 0)

    def test_missing_exchange_dir_exits_nonzero(self):
        """Missing exchange directory should exit with code 1."""
        from groai_fi_datastore_shared.Binance.cli.list_symbols import run

        with patch("sys.argv", ["binance-list-symbols",
                                "--path", str(self.price_root),
                                "--exchange", "NonExistent"]):
            with self.assertRaises(SystemExit) as ctx:
                run()
            self.assertEqual(ctx.exception.code, 1)

    def test_single_symbol_listed(self):
        """Single symbol should appear in output."""
        self._make_symbol("BTCUSDT", num_parts=1, last_date="2024-03-15")

        from groai_fi_datastore_shared.Binance.cli.list_symbols import run

        captured = StringIO()
        with patch("sys.argv", ["binance-list-symbols",
                                "--path", str(self.price_root),
                                "--exchange", self.exchange]):
            with patch("sys.stdout", captured):
                run()

        output = captured.getvalue()
        self.assertIn("BTCUSDT", output)

    def test_multiple_symbols_all_listed(self):
        """All three symbols should appear in output."""
        for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
            self._make_symbol(sym, num_parts=2, last_date="2024-04-01")

        from groai_fi_datastore_shared.Binance.cli.list_symbols import run

        captured = StringIO()
        with patch("sys.argv", ["binance-list-symbols",
                                "--path", str(self.price_root),
                                "--exchange", self.exchange]):
            with patch("sys.stdout", captured):
                run()

        output = captured.getvalue()
        for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
            self.assertIn(sym, output, f"{sym} should appear in listing")

    def test_merged_status_shown_for_single_part(self):
        """A symbol with exactly 1 part file should show '(merged)'."""
        self._make_symbol("BTCUSDT", num_parts=1)

        from groai_fi_datastore_shared.Binance.cli.list_symbols import run

        captured = StringIO()
        with patch("sys.argv", ["binance-list-symbols",
                                "--path", str(self.price_root),
                                "--exchange", self.exchange]):
            with patch("sys.stdout", captured):
                run()

        output = captured.getvalue()
        self.assertIn("(merged)", output)

    def test_pending_merge_status_for_many_parts(self):
        """A symbol with more than 50 parts should show '(⚠ pending merge)'."""
        self._make_symbol("BTCUSDT", num_parts=55)

        from groai_fi_datastore_shared.Binance.cli.list_symbols import run

        captured = StringIO()
        with patch("sys.argv", ["binance-list-symbols",
                                "--path", str(self.price_root),
                                "--exchange", self.exchange]):
            with patch("sys.stdout", captured):
                run()

        output = captured.getvalue()
        self.assertIn("pending merge", output)

    def test_part_count_in_output(self):
        """Part count should be accurate in listing output."""
        self._make_symbol("BTCUSDT", num_parts=3)

        from groai_fi_datastore_shared.Binance.cli.list_symbols import run

        captured = StringIO()
        with patch("sys.argv", ["binance-list-symbols",
                                "--path", str(self.price_root),
                                "--exchange", self.exchange]):
            with patch("sys.stdout", captured):
                run()

        output = captured.getvalue()
        # "parts=3" should appear somewhere in the output
        self.assertIn("parts=3", output)


class TestRemoveSymbol(unittest.TestCase):
    """Test binance-remove-symbol (local filesystem) behaviour."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix="groai_test_remove_symbol_")
        self.price_root = Path(self._tmpdir) / "prices_v3.parquet"
        self.exchange   = "Binance"
        self.exchange_dir = self.price_root / f"exchange={self.exchange}"

    def tearDown(self):
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _make_symbol(self, symbol: str) -> Path:
        symbol_dir = self.exchange_dir / f"symbol={symbol}"
        symbol_dir.mkdir(parents=True, exist_ok=True)
        dates = pd.date_range("2024-01-01", periods=60, freq="1min")
        df = pd.DataFrame({"date": dates, "close": 100.0})
        df.to_parquet(symbol_dir / "part.00000.parquet", engine="pyarrow", index=False)
        return symbol_dir

    # ── tests ─────────────────────────────────────────────────────────────────

    def test_nonexistent_symbol_exits_zero(self):
        """Trying to remove a symbol that doesn't exist should exit 0 cleanly."""
        from groai_fi_datastore_shared.Binance.cli.remove_symbol import run

        with patch("sys.argv", ["binance-remove-symbol",
                                "--symbol", "GHOST",
                                "--path", str(self.price_root),
                                "--yes"]):
            with self.assertRaises(SystemExit) as ctx:
                run()
            self.assertEqual(ctx.exception.code, 0)

    def test_remove_with_yes_flag_deletes_directory(self):
        """--yes flag should delete the symbol directory without prompting."""
        symbol_dir = self._make_symbol("BTCUSDT")
        self.assertTrue(symbol_dir.exists(), "Precondition: directory must exist")

        from groai_fi_datastore_shared.Binance.cli.remove_symbol import run

        with patch("sys.argv", ["binance-remove-symbol",
                                "--symbol", "BTCUSDT",
                                "--path", str(self.price_root),
                                "--yes"]):
            run()

        self.assertFalse(symbol_dir.exists(), "Symbol directory should be deleted")

    def test_remove_does_not_affect_other_symbols(self):
        """Removing one symbol should leave its sibling directories untouched."""
        self._make_symbol("BTCUSDT")
        eth_dir = self._make_symbol("ETHUSDT")

        from groai_fi_datastore_shared.Binance.cli.remove_symbol import run

        with patch("sys.argv", ["binance-remove-symbol",
                                "--symbol", "BTCUSDT",
                                "--path", str(self.price_root),
                                "--yes"]):
            run()

        self.assertTrue(eth_dir.exists(), "ETHUSDT directory should be untouched")
        self.assertFalse((self.exchange_dir / "symbol=BTCUSDT").exists(),
                         "BTCUSDT directory should be removed")

    def test_abort_without_yes_and_wrong_confirmation(self):
        """Typing the wrong symbol name at the confirmation prompt should abort."""
        symbol_dir = self._make_symbol("BTCUSDT")

        from groai_fi_datastore_shared.Binance.cli.remove_symbol import run

        # Simulate typing the wrong answer at the prompt
        with patch("builtins.input", return_value="WRONG"):
            with patch("sys.argv", ["binance-remove-symbol",
                                    "--symbol", "BTCUSDT",
                                    "--path", str(self.price_root)]):
                with self.assertRaises(SystemExit) as ctx:
                    run()
                self.assertEqual(ctx.exception.code, 0)

        self.assertTrue(symbol_dir.exists(),
                        "Directory should NOT be removed after wrong confirmation")

    def test_correct_confirmation_deletes_directory(self):
        """Typing the exact symbol name at the prompt should proceed with deletion."""
        symbol_dir = self._make_symbol("BTCUSDT")

        from groai_fi_datastore_shared.Binance.cli.remove_symbol import run

        with patch("builtins.input", return_value="BTCUSDT"):
            with patch("sys.argv", ["binance-remove-symbol",
                                    "--symbol", "BTCUSDT",
                                    "--path", str(self.price_root)]):
                run()

        self.assertFalse(symbol_dir.exists(), "Directory should be removed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
