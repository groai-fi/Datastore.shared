import sys
import time
import hashlib
import requests
import hmac
from .config import BinanceConfig, get_default_config
from requests.exceptions import ConnectionError

try:
    from urllib import urlencode
# python3
except ImportError:
    from urllib.parse import urlencode

# Get recv_window from config
recv_window = get_default_config().recv_window


class BinanceClient:
    BASE_URL = "https://www.binance.com/api/v1"
    # BASE_URL = "https://testnet.binance.vision/api"
    BASE_URL_V1 = "https://api.binance.com/sapi/v1"
    BASE_URL_V3 = "https://api.binance.com/api/v3"
    # BASE_URL_V3 = BASE_URL + "v3/order"
    PUBLIC_URL = "https://www.binance.com/exchange/public/product"

    def __init__(self, key, secret, exch_mode="SpotTest"):
        self.exch_mode = exch_mode

        if key is None:
            print('Binance key/secret cannot be empty')
            time.sleep(3)
            sys.exit()
        if secret is None:
            print('Binance key/secret cannot be empty')
            time.sleep(3)
            sys.exit()

        self.key = key
        self.secret = secret

        if exch_mode == "SpotTest":  # SpotTest Network
            # MUST READ
            # https://testnet.binance.vision
            self.BASE_URL_V3 = "https://testnet.binance.vision/api/v3"

    def ping(self):
        path = "%s/ping" % self.BASE_URL_V3
        return requests.get(path, timeout=30, verify=True).json()

    def system_status(self):
        path = "%s/system/status" % self.BASE_URL_V1
        return requests.get(path, timeout=30, verify=True).json()

    def get_history(self, market, limit=50):
        path = "%s/historicalTrades" % self.BASE_URL
        params = {"symbol": market, "limit": limit}
        return self._get_no_sign(path, params)

    def get_trades(self, market, limit=50):
        path = "%s/trades" % self.BASE_URL
        params = {"symbol": market, "limit": limit}
        return self._get_no_sign(path, params)

    def get_klines(self, market, interval, startTime, endTime):
        path = "%s/klines" % self.BASE_URL_V3
        params = {"symbol": market, "interval": interval, "startTime": startTime, "endTime": endTime, "limit": 1000}
        return self._get_no_sign(path, params)

    def get_24h_ticker(self, market):
        path = "%s/ticker/24hr" % self.BASE_URL_V3
        params = {"symbol": market}
        return self._get_no_sign(path, params)

    def get_ticker(self, market):
        path = "%s/ticker/price" % self.BASE_URL_V3
        params = {"symbol": market}
        return self._get_no_sign(path, params)

    def get_order_books(self, market, limit=50):
        path = "%s/depth" % self.BASE_URL_V3
        params = {"symbol": market, "limit": limit}
        return self._get_no_sign(path, params)

    def get_account(self):
        path = "%s/account" % self.BASE_URL_V3
        return self._get(path, {})

    def get_products(self):
        return requests.get(self.PUBLIC_URL, timeout=30, verify=True).json()

    def get_server_time(self):
        path = "%s/time" % self.BASE_URL_V3
        return requests.get(path, timeout=30, verify=True).json()

    def get_exchange_info(self):
        path = "%s/exchangeInfo" % self.BASE_URL
        return requests.get(path, timeout=30, verify=True).json()

    def get_open_orders(self, market, limit=100):
        path = "%s/openOrders" % self.BASE_URL_V3
        params = {"symbol": market}
        return self._get(path, params)

    def get_my_trades(self, market, limit=50):
        path = "%s/myTrades" % self.BASE_URL_V3
        params = {"symbol": market, "limit": limit}
        return self._get(path, params)

    def buy_limit(self, market, quantity, rate):
        path = "%s/order" % self.BASE_URL_V3
        params = self._order(market, quantity, "BUY", rate)
        return self._post(path, params)

    def sell_limit(self, market, quantity, rate):
        path = "%s/order" % self.BASE_URL_V3
        params = self._order(market, quantity, "SELL", rate)
        return self._post(path, params)

    def buy_margin_limit(self, market, quantity, rate, sideEffectType=None):
        path = "%s/margin/order" % self.BASE_URL_V1
        params = self._order(market, quantity, "BUY", rate, sideEffectType)
        return self._post_margin(path, params)

    def sell_margin_limit(self, market, quantity, rate, sideEffectType=None):
        path = "%s/margin/order" % self.BASE_URL_V1
        params = self._order(market, quantity, "SELL", rate, sideEffectType)
        return self._post_margin(path, params)

    def buy_market(self, market, quantity):
        path = "%s/order" % self.BASE_URL_V3
        params = self._order(market, quantity, "BUY")
        return self._post(path, params)

    def sell_market(self, market, quantity):
        path = "%s/order" % self.BASE_URL_V3
        params = self._order(market, quantity, "SELL")
        return self._post(path, params)

    def buy_margin_market(self, market, quantity, sideEffectType=None):
        path = "%s/margin/order" % self.BASE_URL_V1
        params = self._order(market, quantity, "BUY", sideEffectType=sideEffectType)
        return self._post_margin(path, params)

    def sell_margin_market(self, market, quantity, sideEffectType=None):
        path = "%s/margin/order" % self.BASE_URL_V1
        params = self._order(market, quantity, "SELL", sideEffectType=sideEffectType)
        return self._post_margin(path, params)

    def borrow(self, market: str, amount: float):
        path = "%s/margin/loan" % self.BASE_URL_V1
        params = {"asset": market, 'amount': amount}
        return self._post_margin(path, params)

    def maxBorrowable(self, market: str, isolatedSymbol: str):
        path = "%s/margin/maxBorrowable" % self.BASE_URL_V1
        params = {"asset": market, "isolatedSymbol": isolatedSymbol}
        return self._get(path, params)

    async def repay(self, market: str, amount: float):
        path = "%s/margin/repay" % self.BASE_URL_V1
        params = {"asset": market, 'amount': amount}
        return self._post_margin(path, params)

    def get_trade_fee(self, symbol):
        path = "%s/asset/tradeFee" % self.BASE_URL_V1
        params = {"symbol": symbol}
        return self._get(path, params)

    def get_margin_asset(self):
        """
        {
            "borrowEnabled": true,
            "marginLevel": "11.64405625",
            "totalAssetOfBtc": "6.82728457",
            "totalLiabilityOfBtc": "0.58633215",
            "totalNetAssetOfBtc": "6.24095242",
            "tradeEnabled": true,
            "transferEnabled": true,
            "userAssets": [
                {
                    "asset": "BTC",
                    "borrowed": "0.00000000",
                    "free": "0.00499500",
                    "interest": "0.00000000",
                    "locked": "0.00000000",
                    "netAsset": "0.00499500"
                },
                {
                    "asset": "BNB",
                    "borrowed": "201.66666672",
                    "free": "2346.50000000",
                    "interest": "0.00000000",
                    "locked": "0.00000000",
                    "netAsset": "2144.83333328"
                },
                {
                    "asset": "ETH",
                    "borrowed": "0.00000000",
                    "free": "0.00000000",
                    "interest": "0.00000000",
                    "locked": "0.00000000",
                    "netAsset": "0.00000000"
                },
                {
                    "asset": "USDT",
                    "borrowed": "0.00000000",
                    "free": "0.00000000",
                    "interest": "0.00000000",
                    "locked": "0.00000000",
                    "netAsset": "0.00000000"
                }
            ]
        }
        """
        path = "%s/margin/account" % self.BASE_URL_V1
        params = {}
        return self._get(path, params)

    def enable_isolated(self, base: str, quote: str):
        path = "%s/margin/isolated/create" % self.BASE_URL_V1
        params = {"base": base, "quote": quote}
        return self._post(path, params)

    def query_order(self, market, orderId):
        path = "%s/order" % self.BASE_URL_V3
        params = {"symbol": market, "orderId": orderId}
        return self._get(path, params)

    def query_margin_order(self, market, orderId):
        path = "%s/margin/order" % self.BASE_URL_V1
        params = {"symbol": market, "orderId": orderId}
        return self._get(path, params)

    def cancel(self, market, order_id):
        path = "%s/order" % self.BASE_URL_V3
        params = {"symbol": market, "orderId": order_id}
        return self._delete(path, params)

    def cancel_margin(self, market, order_id):
        path = "%s/margin/order" % self.BASE_URL_V3
        params = {"symbol": market, "orderId": order_id}
        return self._delete(path, params)

    def _get_no_sign(self, path, params={}):
        query = urlencode(params)
        url = "%s?%s" % (path, query)
        # return requests.get(url, timeout=30, verify=True).json()

        nb_tries = 10
        while True:
            nb_tries -= 1
            try:
                # Request url
                result = requests.get(url, timeout=30, verify=True).json()
                break
            except ConnectionError as err:
                if nb_tries == 0:
                    raise err
                else:
                    time.sleep(1)
        return result

    def _sign(self, params={}):
        data = params.copy()

        ts = int(1000 * time.time())
        data.update({"timestamp": ts})
        h = urlencode(data)
        b = bytearray()
        b.extend(self.secret.encode())
        signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        data.update({"signature": signature})
        return data

    def _get(self, path, params={}):
        params.update({"recvWindow": recv_window})
        query = urlencode(self._sign(params))
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.key}
        # return requests.get(url, headers=header, timeout=30, verify=True).json()

        nb_tries = 10
        while True:
            nb_tries -= 1
            try:
                # Request url
                _result = requests.get(url, headers=header, timeout=30, verify=True)
                result = _result.json()
                break
            except ConnectionError as err:
                if nb_tries == 0:
                    raise err
                else:
                    time.sleep(1)
        return result

    def _post_margin(self, path, params={}):
        params.update({"recvWindow": recv_window})
        query = urlencode(self._sign(params))
        url = "%s" % (path)
        header = {"X-MBX-APIKEY": self.key}
        # return requests.post(url, headers=header, data=query, timeout=30, verify=True).json()

        nb_tries = 10
        while True:
            nb_tries -= 1
            try:
                # Request url
                result = requests.post(url, headers=header, params=query, timeout=30, verify=True).json()
                break
            except ConnectionError as err:
                if nb_tries == 0:
                    raise err
                else:
                    time.sleep(1)
        return result

    def _post(self, path, params={}):
        params.update({"recvWindow": recv_window})
        query = urlencode(self._sign(params))
        url = "%s" % (path)
        header = {"X-MBX-APIKEY": self.key}
        # return requests.post(url, headers=header, data=query, timeout=30, verify=True).json()

        nb_tries = 10
        while True:
            nb_tries -= 1
            try:
                # Request url
                result = requests.post(url, headers=header, data=query, timeout=30, verify=True).json()
                break
            except ConnectionError as err:
                if nb_tries == 0:
                    raise err
                else:
                    time.sleep(1)
        return result

    def _order(self, market, quantity, side, rate=None, sideEffectType=None):
        params = {}

        if rate is not None:
            params["type"] = "LIMIT"
            params["price"] = self._format_price(rate)
            params["timeInForce"] = "GTC"
        else:
            params["type"] = "MARKET"

        if sideEffectType is not None:
            params["sideEffectType"] = sideEffectType

        params["symbol"] = market
        params["side"] = side
        params["quantity"] = self._format_qty(quantity)

        return params

    def _delete(self, path, params={}):
        params.update({"recvWindow": recv_window})
        query = urlencode(self._sign(params))
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.key}
        # return requests.delete(url, headers=header, timeout=30, verify=True).json()

        nb_tries = 10
        while True:
            nb_tries -= 1
            try:
                # Request url
                result = requests.delete(url, headers=header, timeout=30, verify=True).json()
                break
            except ConnectionError as err:
                if nb_tries == 0:
                    raise err
                else:
                    time.sleep(1)
        return result

    def _format_price(self, price):
        return "{:.8f}".format(price)

    def _format_qty(self, qty):
        return "{:.8f}".format(float(qty))
