import unittest
from datetime import datetime

# Package imports (no sys.path hacks needed when installed via `uv sync`)
from groai_fi_datastore_shared.Binance import BinanceMarketDataDownloader


class TestBinanceKlines(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            # _get_client() is the lazy singleton — calling it initialises the
            # Binance API client on first access, without hitting it at import time.
            cls.client = BinanceMarketDataDownloader._get_client()
        except Exception as e:
            print(f"Failed to initialise Binance client: {e}")
            cls.client = None

    def test_get_klines_user_request(self):
        if not self.client:
            self.fail("Client could not be initialized")

        symbol = "BCHUSDT"
        kline_tframe = '1m'
        from_millis_str = '1769117940000'
        step_millis_str = '1769146740000'

        print("\n=== Testing User Parameters ===")
        print(f"Symbol: {symbol}")
        print(f"Timeframe: {kline_tframe}")
        print(f"From (ms string): {from_millis_str}")
        print(f"Step (ms string): {step_millis_str}")

        # 1. Test original code usage (Positional arguments)
        print("\n[Test 1] Positional Arguments (as in usage):")
        try:
            klines = self.client.get_klines(symbol, kline_tframe, from_millis_str, step_millis_str)
            print(f"SUCCESS. Returned {len(klines)} klines.")
        except Exception as e:
            print(f"FAILED: {type(e).__name__}: {e}")

        # 2. Test kwargs with original values (ms strings)
        print("\n[Test 2] Keyword Arguments with MS Strings:")
        try:
            # python-binance often expects ints for timestamps, but let's see
            klines = self.client.get_klines(
                symbol=symbol,
                interval=kline_tframe,
                startTime=from_millis_str,
                endTime=step_millis_str
            )
            print(f"SUCCESS. Returned {len(klines)} klines.")
        except Exception as e:
            print(f"FAILED: {type(e).__name__}: {e}")

        # 3. Test kwargs with MS integers
        print("\n[Test 3] Keyword Arguments with MS Integers:")
        try:
            klines = self.client.get_klines(
                symbol=symbol,
                interval=kline_tframe,
                startTime=int(from_millis_str),
                endTime=int(step_millis_str)
            )
            print(f"SUCCESS. Returned {len(klines)} klines.")
            if len(klines) > 0:
                first_open_time = klines[0][0]
                print(f"First candle Open Time: {first_open_time}")
                print(f"Is it matching start? {int(first_open_time) >= int(from_millis_str)}")
        except Exception as e:
            print(f"FAILED: {type(e).__name__}: {e}")

        # 4. Test kwargs with Seconds integers (Divide by 1000)
        print("\n[Test 4] Keyword Arguments with Seconds Integers (values/1000):")
        try:
            from_sec = int(int(from_millis_str) / 1000)
            step_sec = int(int(step_millis_str) / 1000)
            klines = self.client.get_klines(
                symbol=symbol,
                interval=kline_tframe,
                startTime=from_sec,
                endTime=step_sec
            )
            print(f"SUCCESS. Returned {len(klines)} klines.")
            if len(klines) > 0:
                print(f"First candle Open Time: {klines[0][0]}")
        except Exception as e:
            print(f"FAILED: {type(e).__name__}: {e}")

        # 5. Test with Recent PAST data (to prove it works)
        print("\n[Test 5] Recent Valid Data (24 hours ago):")
        try:
            now_ms = int(datetime.now().timestamp() * 1000)
            start_ms = now_ms - (24 * 60 * 60 * 1000)  # 24 hours ago
            end_ms = now_ms

            klines = self.client.get_klines(
                symbol=symbol,
                interval=kline_tframe,
                startTime=start_ms,
                endTime=end_ms
            )
            print(f"SUCCESS. Returned {len(klines)} klines using recent timestamp.")
            if len(klines) > 0:
                print(f"Sample data [0]: {klines[0]}")
        except Exception as e:
            print(f"FAILED: {type(e).__name__}: {e}")

        # Analysis of User's Timestamp
        print("\n=== Analysis ===")
        user_ts = 1769117940000
        user_date = datetime.fromtimestamp(user_ts / 1000)
        current_date = datetime.now()
        print(f"User Timestamp: {user_ts} -> {user_date}")
        print(f"Current Time:   {current_date}")
        if user_date > current_date:
            print("CONCLUSION: The user timestamp is in the FUTURE. That is why no data is returned.")
        else:
            print("CONCLUSION: The user timestamp is in the past.")


if __name__ == '__main__':
    unittest.main()
