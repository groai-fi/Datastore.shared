import os
import sys
import time
import pickle
import random
from pprint import pformat
from decimal import Decimal
from time import sleep
import pandas as pd
from copy import deepcopy

from pathlib import Path
from .config import BinanceConfig, get_default_config
from datetime import datetime as dt
from datetime import timedelta
from joblib import Parallel, delayed
from .utils import readable_error, normalize_fraction, least_significant_digit_power, pretty_dict, d_round, d

from binance.client import Client, BaseClient
from typing import Dict, Optional, Union, Any
from .email_utils import send_mail
from .enums import OrderStatus
from .schema import last_tick_columns, last_tick_columns_type


class BinanceClient(Client):
    def __init__(
            self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
            requests_params: Optional[Dict[str, Any]] = None, tld: str = 'com',
            base_endpoint: str = BaseClient.BASE_ENDPOINT_DEFAULT, testnet: bool = False,
            private_key: Optional[Union[str, Path]] = None, private_key_pass: Optional[str] = None
    ):
        super().__init__(api_key, api_secret,
            requests_params, tld,
            base_endpoint, testnet,
            private_key, private_key_pass)


    def get_isolated_margin_tier(self, **params):
        """
        Get isolated margin tier data collection with any tier as https://www.binance.com/en/margin-data

        :params symbol: symbol to be checked
            - type: string
            - required: true

        :returns: API response

            [
                {
                    "symbol": "BTCUSDT",
                    "tier": 1,
                    "effectiveMultiple": "10",
                    "initialRiskRatio": "1.111",
                    "liquidationRiskRatio": "1.05",
                    "baseAssetMaxBorrowable": "9",
                    "quoteAssetMaxBorrowable": "70000"
                }
            ]
        """
        return self._request_margin_api('get', 'margin/isolatedMarginTier', True, data=params)


class BinanceOrder:
    def __init__(self, exch_mode, _logger, config: BinanceConfig = None):
        
        if config is None:
            config = get_default_config()
        
        self.config = config

        if exch_mode not in ['SpotTest', 'SpotAPI']:
            _logger.error(f'[exch_mode] exch_mode can only be SpotTest or SpotAPI but got {exch_mode}, bot existing')
            time.sleep(3)
            sys.exit()

        if exch_mode == 'SpotAPI':
            BINANCE_API_KEY = config.api_key
            BINANCE_API_SECRET = config.api_secret
        else:
            BINANCE_API_KEY = config.api_key_test
            BINANCE_API_SECRET = config.api_secret_test

        # logger.info(f"BINANCE_API_KEY used: {BINANCE_API_KEY}")

        self.client = BinanceClient(api_key=BINANCE_API_KEY,
                                    api_secret=BINANCE_API_SECRET,
                                    testnet=(exch_mode == "SpotTest"))

        # 有些無法用 SpotTest 去取得資訊，比如 get_trade_fee
        self.client_LIVE = BinanceClient(api_key=config.api_key,
                                         api_secret=config.api_secret)

        if exch_mode == 'SpotTest':
            # Reference
            # https://github.com/sammchardy/python-binance/issues/757#issuecomment-813117154
            self.client.API_URL = "https://testnet.binance.vision/api"

        self.logger = _logger

        self.__spec = {}
        self.makerCommission = {}
        self.takerCommission = {}
        self.exch_mode = exch_mode

        self.is_validated = False

    async def ping(self) -> bool:
        re = None
        try:
            re = self.client.get_system_status()
            if 'status' not in re:
                return False
            elif re['status'] == 0:
                return True

        except Exception as e:
            err = 'BinanceOrder ping system_status: {0}'.format(re if re is not None else '')
            self.logger.error(f"[Order] {err} {readable_error(e, __file__)}")

    def send_mail(self, subject, content):
        try:
            subject = f'{subject} {dt.now()}'
            receivers = self.config.send_mail_receiver
            if receivers:
                receivers = receivers.split(',')
            else:
                receivers = []
            send_mail(receivers, subject, content)
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] Sendmail >> {err}")

    def get_decimal_price_qty(self, symbol) -> dict:
        # ====== Set decimal Precision ======
        # dlr_agent.env.ds is enabled only after 'dlr_agent.get_model'
        decimal_qty_view = self.get_qty_decimal_view(symbol)
        decimal_price_view = self.get_price_decimal_view(symbol)
        decimal_qty = self.get_qty_decimal(symbol)
        decimal_price = self.get_price_decimal(symbol)

        return {
            "decimal_qty_view": decimal_qty_view,
            "decimal_price_view": decimal_price_view,
            "decimal_qty": decimal_qty,
            "decimal_price": decimal_price
        }
        # self.ds.set_decimal_price_qty(decimal_qty_view, decimal_price_view, decimal_qty, decimal_price)

    async def buy_limit(self, symbol, quantity, buy_price) -> (str, Decimal, Decimal, dict):
        try:
            order = self.client.order_limit_buy(symbol=symbol, quantity=quantity, price=buy_price)
            order.setdefault("fills", "...")
            self.logger.info(f'[Orders] Binance Orders.buy_limit ==>\n{order}')

            if 'msg' in order:
                self.logger.error('[Orders] Binance orders.buy_limit ==>\n{0}'.format(order['msg']))
                raise Exception(order['msg'])

            orderId = order.get('orderId', None)
            executedQty = d(order.get('executedQty', 0))
            if 'cummulativeQuoteQty' in order:
                executedAmt = d(order.get('cummulativeQuoteQty', 0))
            elif 'executedAmt' in order:
                executedAmt = d(order.get('executedAmt', 0))
            else:
                raise Exception('ExecutedAmt not found')

            return orderId, executedQty, executedAmt, order
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] buy_limit >> {err}")
            self.send_mail('ERROR {0} Binance API'
                           ''.format(symbol), err)

    async def sell_limit(self, symbol, quantity, sell_price) -> (str, Decimal, Decimal, dict):
        try:
            order = self.client.order_limit_sell(symbol=symbol, quantity=quantity, price=sell_price)
            order.setdefault("fills", "...")
            self.logger.info('[Orders] Binance orders.sell_limit ==>\n{0}'.format(order))

            if 'msg' in order:
                self.logger.error('[Orders] Binance sell_limit ==>\n{0}'.format(order['msg']))
                raise Exception(order['msg'])

            orderId = order.get('orderId', None)
            executedQty = d(order.get('executedQty', 0))
            if 'cummulativeQuoteQty' in order:
                executedAmt = d(order.get('cummulativeQuoteQty', 0))
            elif 'executedAmt' in order:
                executedAmt = d(order.get('executedAmt', 0))
            else:
                raise Exception(f'ExecutedAmt not found:{order}')

            return orderId, executedQty, executedAmt, order

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] sell_limit >> {err}")
            self.send_mail('ERROR {0} Binance API'
                           ''.format(symbol), err)

    async def buy_to_cover_market(self, symbol, quantity: Decimal) -> (str, Decimal, Decimal, dict):
        """
        這是自動買了後 AUTO_REPAY 版本，手續費是由錢包出，完全還款的狀態。
        如果你要買的數量和還款數量不同，請用 buy_then_repay_market()。
        如果你是交易機器人使用，請使用 buy_then_repay_market(),因為你
        需要買的數量與 repay 數量不同。
        e.g. Short selling 100 BTC，然後 buy_to_cover_market 100 BTC，那手續費所需要會從
        target_asset 的錢裡面扣除，如果裡面沒有多餘的錢，那你會多欠他(debt) 0.1 BTC。
        - 如果你買 100.1 BTC to cover，那他還是會從 target_asset 去扣除 0.1 的 fee, 也就是 fee_of_fee
        因為你 100.1 的 fee_of_fee 手續費是 0.0001
        - 這 function 比較適合把剩下的 position 全部 cover，這樣就是買 100.1 BTC to cover
        如果只有買一部分，而且你要計算是否這交易有賺錢，請用 buy_then_repay_market()
        """
        try:
            return await self.buy_margin_market(symbol, quantity, 'AUTO_REPAY')
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] buy_to_cover_market >> {err}")

    async def buy_then_repay_market(self, symbol, buy_qty: Decimal,
                                    repay_asset: str,
                                    repay_qty: Decimal,
                                    loan_interest: Decimal = d(0),
                                    **kwargs) -> (str, Decimal, Decimal, dict):
        """
        這個功能跟 buy_to_cover_market 不同的是在於買的數量和 repay 的數量不同。
        e.g. 買了 100.1 BTC (margin cover), 這樣還款 100 BTC，手續費 0.1 BTC，這要好比你買了 100.1 BTC
        然後因為手續費，你拿到的 received_qty 需要先扣掉手續費，這樣邏輯就跟 spot 一樣。
        而且，你錢包裡必須現有 0.1 BTC 來支付買 100.1 BTC, 然後買多的 0.1 BTC 也算
        是放回錢包。

        loan_interest 處理方式：
        我們先不把他當成交易成本，看看後續如何處理，所以預設是 d(0)

        loan_interest is None 的時候:
        如果你不指定 repay_interest, 那我們會自動計算
        repay_qty_interest = repay_qty + repay_interest
        """
        try:
            if loan_interest is None:
                acct = self.get_account_balance(repay_asset, 'isolated', **kwargs)
                loan_interest = d(acct[repay_asset]['interest'])

            repay_qty_interest = repay_qty + loan_interest
            buy_qty = buy_qty + loan_interest
            orderId, executedQty, executedAmt, order = await self.buy_margin_market(symbol, buy_qty)

            async def is_filled_and_repay(filled_status):
                if filled_status and filled_status == 'FILLED':
                    # 條件成立才執行一次
                    self.logger.info(f"[Orders] repaying {repay_qty_interest} {repay_asset}")
                    tranId = await self.repay_loan(repay_asset, repay_qty_interest, symbol)
                    order['marginBuyBorrowAsset'] = repay_asset
                    order['marginRepayQtyTotal'] = repay_qty_interest
                    order['marginRepayQty'] = repay_qty
                    order['marginRepayInterest'] = loan_interest
                    order['marginRepayTranId'] = tranId
                    return True
                return False

            i = 0
            while True:
                sleep(2)
                re = self.get_order(symbol, orderId, 'isolated')
                status = re.get('filledStatus').name
                if await is_filled_and_repay(status):
                    break

                i += 1
                if i > 10:
                    raise Exception('BinanceOrder.buy_then_repay_market is not filled')

            return orderId, executedQty, executedAmt, order

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] buy_then_repay_market >> {err}")

    async def repay_loan(self, asset, amount, isolated_symbol):
        """
        單純 repay margin loan, 沒有其他邏輯。
        """
        try:
            repay_re = self.client.repay_margin_loan(asset=asset,
                                                     amount=amount,
                                                     symbol=isolated_symbol,
                                                     isIsolated=True)
            self.logger.info('[Orders] Binance order repay_loan ==>\n{0}'.format(repay_re))

            if 'msg' in repay_re:
                self.logger.error('[Orders] Binance repay_loan ==>\n{0}'.format(repay_re['msg']))
                raise Exception(repay_re['msg'])

            tranId = repay_re.get('tranId', None)

            return tranId
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] repay_loan >> {err}")
            self.send_mail(f'ERROR BinanceOrder.repay_loan {asset}', err)

    async def buy_to_cover_limit(self, symbol, quantity, buyPrice) -> (str, Decimal, Decimal, dict):
        """
        這個跟上面 buy_to_cover_market 是一樣的邏輯，如果你要 買的和還款分開
        那就是請用 buy_then_repay_limit。
        """
        try:
            return await self.buy_margin_limit(symbol, quantity, buyPrice, 'AUTO_REPAY')
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] buy_to_cover_limit >> {err}")

    async def buy_margin_limit(self, symbol, quantity, buyPrice, sideEffectType=None) -> (str, Decimal, Decimal, dict):
        try:
            order = self.client.create_margin_order(symbol=symbol,
                                                    quantity=quantity,
                                                    side="BUY",
                                                    price=buyPrice,
                                                    sideEffectType=sideEffectType,
                                                    type="LIMIT",
                                                    isIsolated=True)
            self.logger.info('[Orders] Binance order buy_margin_limit ==>\n{0}'.format(order))

            if 'msg' in order:
                self.logger.error('Binance buy_margin_limit ==>\n{0}'.format(order['msg']))
                raise Exception(order['msg'])

            orderId = order.get('orderId', None)
            executedQty = d(order.get('executedQty', 0))
            if 'cummulativeQuoteQty' in order:
                executedAmt = d(order.get('cummulativeQuoteQty', 0))
            elif 'executedAmt' in order:
                executedAmt = d(order.get('executedAmt', 0))
            else:
                raise Exception(f'ExecutedAmt not found:{order}')

            return orderId, executedQty, executedAmt, order
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] buy_margin_limit >> {err}")
            self.send_mail('ERROR {0} Binance API'
                           ''.format(symbol), err)

    async def short_selling_limit(self, symbol, quantity, short_sell_price) -> (str, Decimal, Decimal, dict):
        try:
            return await self.sell_margin_limit(symbol, quantity, short_sell_price, 'MARGIN_BUY')
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] short_selling_limit >> {err}")

    async def sell_margin_limit(self, symbol, quantity, sell_price, sideEffectType=None) -> (
    str, Decimal, Decimal, dict):
        try:
            order = self.client.create_margin_order(symbol=symbol,
                                                    quantity=quantity,
                                                    side="SELL",
                                                    price=sell_price,
                                                    sideEffectType=sideEffectType,
                                                    type="LIMIT",
                                                    isIsolated=True)
            self.logger.info('[Orders] Binance order sell_margin_limit ==>\n{0}'.format(order))

            if 'msg' in order:
                self.logger.error('[Orders] Binance sell_margin_limit ==>\n{0}'.format(order['msg']))
                raise Exception(order['msg'])

            orderId = order.get('orderId', None)
            executedQty = d(order.get('executedQty', 0))
            if 'cummulativeQuoteQty' in order:
                executedAmt = d(order.get('cummulativeQuoteQty', 0))
            elif 'executedAmt' in order:
                executedAmt = d(order.get('executedAmt', 0))
            else:
                raise Exception(f'ExecutedAmt not found:{order}')

            return orderId, executedQty, executedAmt, order
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] sell_margin_limit >> {err}")
            self.send_mail(f'ERROR {symbol} Binance API', err)

    async def buy_market(self, symbol, quantity, **kwargs) -> (str, Decimal, Decimal, dict):
        try:
            order = self.client.order_market_buy(symbol=symbol, quantity=quantity)
            order.setdefault("fills", "...")
            self.logger.info(f'[Orders] Binance orders.buy_market ==>\n{order}')

            if 'msg' in order:
                self.logger.error(f"[Orders] Binance buy_market ==>\n{order.get('msg')}")
                raise Exception(order.get('msg'))

            orderId = order.get("orderId", None)
            executedQty = d(order.get('executedQty', 0))

            if 'cummulativeQuoteQty' in order:
                executedAmt = d(order.get("cummulativeQuoteQty", 0))
            elif 'executedAmt' in order:
                executedAmt = d(order.get("executedAmt", 0))
            else:
                raise Exception(f"ExecutedAmt not found:{order}")

            return orderId, executedQty, executedAmt, order
        except Exception as e:
            err = readable_error(e, __file__)
            err_str = f"[BinanceOrders] buy_market >> symbol:{symbol}, qty:quantity\n{err}"
            self.logger.error(err_str)
            self.send_mail(f"ERROR {symbol} Binance API", err_str)

    async def get_server_time(self):
        try:
            re = self.client.get_server_time()

            return re
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] get_server_time >> {err}")

    async def sell_market(self, symbol, quantity, **kwargs):
        try:
            order = self.client.order_market_sell(symbol=symbol, quantity=quantity)
            order.setdefault("fills", "...")
            self.logger.info(f'[Orders] Binance order sell_market ==>\n{order}')

            if 'msg' in order:
                self.logger.error(f'[Orders] Binance sell_market ==>\n{order.get("msg")}')

            orderId = order.get('orderId', None)
            executedQty = d(order.get('executedQty', 0))

            if 'cummulativeQuoteQty' in order:
                executedAmt = d(order.get('cummulativeQuoteQty', 0))
            elif 'executedAmt' in order:
                executedAmt = d(order.get('executedAmt', 0))
            else:
                raise Exception(f'ExecutedAmt not found:{order}')

            return orderId, executedQty, executedAmt, order
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] sell_market >> {err}")
            self.send_mail('ERROR {0} Binance API'
                           ''.format(symbol), err)

    async def buy_margin_market(self, symbol, quantity, sideEffectType=None, **kwargs):
        try:
            order = self.client.create_margin_order(symbol=symbol,
                                                    quantity=quantity,
                                                    side="BUY",
                                                    sideEffectType=sideEffectType,
                                                    type="MARKET",
                                                    isIsolated=True)
            self.logger.info(f'[Orders] Binance order buy_margin_market ==>\n{order}')

            if 'msg' in order:
                self.logger.error(f'[Orders] Binance buy_margin_market ==>\n{order.get("msg")}')
                raise Exception(order['msg'])

            orderId = order.get('orderId', None)
            executedQty = d(order.get('executedQty', 0))

            if 'cummulativeQuoteQty' in order:
                executedAmt = d(order.get('cummulativeQuoteQty', d(0)))
            elif 'executedAmt' in order:
                executedAmt = d(order.get('executedAmt', d(0)))
            else:
                raise Exception(f'ExecutedAmt not found:{order}')

            return orderId, executedQty, executedAmt, order
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] buy_margin_market >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(symbol), err)

    async def get_isolated_margin_tier(self, asset, **kwargs):
        return self.client.get_isolated_margin_tier(symbol=asset)

    async def loan_then_short_selling_market(self, symbol: str, quantity: Decimal, loan_asset: str, **kwargs):
        try:
            tranId = await self.create_loan(loan_asset, symbol, quantity)
            if tranId is None:
                return None, d(0), d(0), {}

            self.logger.info(f">> loan created with tranId {tranId}")
            orderId, executedQty, executedAmt, order = await self.sell_margin_market(symbol, quantity,
                                                                                     sideEffectType=None)
            order['tranId'] = tranId
            return orderId, executedQty, executedAmt, order
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"{symbol} BinanceOrder.loan_then_short_selling_market ==> {err}")
            self.send_mail(f'ERROR {symbol} BinanceOrder.loan_then_short_selling_market', err)

    async def create_loan(self, asset, symbol, qty):
        try:
            loan_re = self.client.create_margin_loan(asset=asset,
                                                     amount=qty,
                                                     symbol=symbol,
                                                     isIsolated=True)
            self.logger.info(f">> BinanceOrder.create_loan ==> {loan_re}")
            tranId = loan_re.get('tranId', None)
            if tranId is None:
                raise Exception(f"BinanceOrder.create_loan failed")
            return tranId
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"{asset} BinanceOrder.create_loan ==> {err}")
            self.send_mail(f'ERROR {asset} BinanceOrder.create_loan', err)
            return None

    async def sell_margin_market(self, symbol, quantity, sideEffectType=None, **kwargs):
        try:
            self.logger.info(f'[Orders] Binance sell_margin_market ==> '
                             f'symbol:{symbol}, qty:{quantity}, sideEffect: {sideEffectType}')
            order = self.client.create_margin_order(symbol=symbol,
                                                    quantity=quantity,
                                                    side="SELL",
                                                    sideEffectType=sideEffectType,
                                                    type="MARKET",
                                                    isIsolated=True)

            self.logger.info(f'[Orders] BinanceOrder.sell_margin_market ==>\n{order}')

            if 'msg' in order:
                self.logger.error('BinanceOrder.sell_margin_market ==> {0}'.format(order['msg']))
                raise Exception(order['msg'])

            orderId = order.get('orderId', None)
            executedQty = d(order.get('executedQty', 0))

            if 'cummulativeQuoteQty' in order:
                executedAmt = d(order.get('cummulativeQuoteQty'))
            elif 'executedAmt' in order:
                executedAmt = d(order.get('executedAmt'))
            else:
                raise Exception(f'ExecutedAmt nor cummulativeQuoteQty not found:{order}')

            return orderId, executedQty, executedAmt, order
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] sell_margin_market >> {err}")
            self.send_mail(f'ERROR {symbol} Binance API', err)

    def cancel_order(self, symbol, orderId):
        try:
            cxl_re = self.client.cancel_order(symbol=symbol,
                                              orderId=orderId)
            if 'msg' in cxl_re:
                self.logger.error('[Orders] Binance cancel_order ==>\n{0}'.format(cxl_re['msg']))
                raise Exception(cxl_re['msg'])

            self.logger.info('[Orders] Binance cancel_order result ==>\n{0}'.format(cxl_re))

            if cxl_re.get('status', None) == 'CANCELLED':
                return True

            return False

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] cancel_order >> {err}")
            self.send_mail('ERROR {0} Binance API'
                           ''.format(symbol), err)

    def cancel_margin_order(self, symbol, orderId):
        try:
            cxl_re = self.client.cancel_margin_order(symbol=symbol,
                                                     orderId=orderId,
                                                     isIsolated=True)
            self.logger.info(f"[Orders] Binance orders.cancel_margin_order result ==>\n{cxl_re}")

            if 'msg' in cxl_re:
                self.logger.error(f"[Orders] Binance cancel_margin_order ==>\n{cxl_re.get('msg')}")
                raise Exception(cxl_re.get('msg'))

            if cxl_re.get('status') == 'CANCELLED':
                return True

            return False

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] cancel_margin_order >> {err}")
            self.send_mail(f"ERROR {symbol} Binance API orderId{orderId}", err)

    def get_order_book(self, symbol, limit=100):
        while True:
            try:
                orders = self.client.get_order_book(symbol=symbol, limit=limit)

                # column names
                cols = ['price', 'volume']

                # create dataframe
                asks = pd.DataFrame(orders.get('asks', None), columns=cols)
                bids = pd.DataFrame(orders.get('bids', None), columns=cols)

                return [asks, bids]

            except Exception as e:
                self.logger.error('Binance get_order_book Exception')
                self.logger.error(readable_error(e, __file__))
                sleep(2)
                self.client = BinanceClient(
                    self.config.api_key,
                    self.config.api_secret,
                    testnet=(self.exch_mode == "SpotTest"))
                continue

    def get_order_book_amount(self, symbol, limit=500):
        try:
            orders = self.client.get_order_book(symbol=symbol, limit=limit)

            ask_top = 0
            ask_bottom = 0
            bid_top = 0
            bid_bottom = 0
            total_bid_amount = 0
            total_ask_amount = 0
            mid, spread, spread_loss = 0, 0, 0
            for i in range(limit):
                lastBid = d(orders['bids'][i][0])  # last buy price (bid)
                lastBidQty = d(orders['bids'][i][1])
                lastAsk = d(orders['asks'][i][0])  # last sell price (ask)
                lastAskQty = d(orders['asks'][i][1])
                if i == 0:
                    ask_bottom = lastAsk
                    bid_top = lastBid

                elif i == limit - 1:
                    ask_top = lastAsk
                    bid_bottom = lastBid

                ask_amount = lastAsk * lastAskQty
                bid_amount = lastBid * lastBidQty
                total_ask_amount += ask_amount
                total_bid_amount += bid_amount

                mid = (ask_bottom + bid_top) / 2
                spread = ask_bottom - bid_top
                spread_loss = spread / bid_top * 100

            return {
                'ask': {'range': [d(ask_top), d(ask_bottom)], 'amount': d(total_ask_amount)},
                'bid': {'range': [d(bid_top), d(bid_bottom)], 'amount': d(total_bid_amount)},
                'mid': d(mid),
                'spread': [d(spread), d(spread_loss)]
            }

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] get_order_book_amount >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(symbol), err)
            return 0, 0

    def get_last_order_book(self, symbol, limit=5) -> (d, d, d, d):
        try:

            orders = self.client.get_order_book(symbol=symbol, limit=limit)
            lastBid = d(orders['bids'][0][0])  # last buy price (bid)
            lastBidQty = d(orders['bids'][0][1])
            lastAsk = d(orders['asks'][0][0])  # last sell price (ask)
            lastAskQty = d(orders['asks'][0][1])

            return lastBid, lastAsk, lastBidQty, lastAskQty

        except Exception as e:
            err = readable_error(e, __file__)
            err_str = f"Please check your APi key: {err}"
            self.logger.error(f"[Orders] get_last_order_book >> {err_str}")
            self.send_mail(f'ERROR {symbol} Binance API', err_str)
            sys.exit(-1)

    def get_order(self, symbol, orderId, spot_margin, verbose=True):
        try:
            if spot_margin == 'spot':
                order = self.client.get_order(symbol=symbol, orderId=orderId)
            elif spot_margin == 'margin':
                order = self.client.get_margin_order(symbol=symbol,
                                                     orderId=orderId,
                                                     isIsolated=False
                                                     )
            elif spot_margin == 'isolated':
                order = self.client.get_margin_order(symbol=symbol,
                                                     orderId=orderId,
                                                     isIsolated=True
                                                     )
            else:
                raise Exception(f"spot_margin can only be spot|margin|isolated but got {spot_margin}")

            if verbose:
                self.logger.info(f'[Orders] Binance orders.get_order ==>\norderId: {orderId}, order: {order}')

            if 'msg' in order:
                # import ipdb; ipdb.set_trace()
                self.logger.error(f"Binance get_order: {order('msg', None)}")
                raise Exception(repr(order('msg', None)))

            orderId = order['orderId']
            reqQty = order['origQty']
            executedQty = d(order['executedQty'])
            price = d(order['price'])
            amt = d(order['cummulativeQuoteQty'])
            if order['status'] == 'NEW':
                filledStatus = OrderStatus.NEW
            elif order['status'] == 'FILLED':
                filledStatus = OrderStatus.FILLED
            elif order['status'] == 'PARTIALLY_FILLED':
                filledStatus = OrderStatus.PARTIALLY_FILLED
            elif order['status'] == 'CANCELLED':
                filledStatus = OrderStatus.CANCELED
            elif order['status'] == 'EXPIRED':
                filledStatus = OrderStatus.EXPIRED
            elif order['status'] == 'REJECTED':
                filledStatus = OrderStatus.FAILED
            else:
                filledStatus = OrderStatus.FAILED

            return {'orderId': orderId, 'reqQty': reqQty, 'executedQty': executedQty, 'executedPrice': price,
                    'executedAmt': amt, 'filledStatus': filledStatus, 'order': order}

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] get_order >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(symbol), err)
            return {'orderId': orderId, 'reqQty': 0.0, 'executedQty': 0.0, 'executedPrice': 0.0,
                    'executedAmt': 0.0, 'filledStatus': OrderStatus.FAILED}

    # Because other exchange doesn't have this method, so we don't use it
    @DeprecationWarning
    def _get_borrowed_balance(self, quote, symbol):
        try:
            acct = self.get_account_balance(quote, 'isolated')
            balance = d(acct['borrowed'])
            _decimal = self.get_qty_decimal(symbol)
            balance = self.truncate_float(balance, _decimal)

            return balance
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] _get_borrowed_balance >> {err}")
            self.send_mail('ERROR {0} Binance API, quote:{1}'.format(symbol, quote), err)
            raise

    def get_last_ticker(self, symbols):
        try:
            # Fetches the ticker price
            # output = Parallel(n_jobs=2)(delayed(self.get_ticker)(symbol) for symbol in self.symbols)
            output = Parallel(n_jobs=2, require='sharedmem')(delayed(self.get_24h_ticker)(symbol) for symbol in symbols)

            ticker24h1 = output[0]
            ticker24h2 = output[1]

            now = dt.now()
            data_df1 = pd.DataFrame([ticker24h1], columns=last_tick_columns)
            data_df1 = data_df1.astype(dtype=last_tick_columns_type)

            data_df1['date'] = now

            # data_df1['date'] = pd.to_datetime(now, unit='ms')
            # data_df1['date'] = utc2LocalDf(data_df1['date'])
            # data_df1['date'] = data_df1['date'].dt.tz_localize('UTC').dt.tz_convert(config.time_zone)

            data_df2 = pd.DataFrame([ticker24h2], columns=last_tick_columns)
            data_df2 = data_df2.astype(dtype=last_tick_columns_type)

            data_df2['date'] = now

            # data_df2['date'] = pd.to_datetime(now, unit='ms')
            # data_df2['date'] = data_df2['date'].dt.tz_localize('UTC').dt.tz_convert(config.time_zone)
            return data_df1, data_df2

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] get_last_ticker >> {err}")
            return None, None

    def get_24h_ticker(self, symbol):
        try:
            ticker = self.client.get_ticker(symbol=symbol)

            return [ticker['closeTime'], d(ticker['lastPrice']),
                    d(ticker['askPrice']), d(ticker['bidPrice'])]

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] get_24h_ticker >> {err}")

    def get_ticker(self, symbol):
        try:
            ticker = self.client.get_ticker(symbol=symbol)
            return [dt.now(), d(ticker['lastPrice'])]

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] get_ticker >> {err}")
            time.sleep(3)
            sys.exit()

    def _get_account(self, symbol=None) -> dict:
        ticker = None
        try:
            ticker = self.client.get_account()
            if 'balances' not in ticker:
                raise Exception('[Orders] def get_account Incorrect return format')

            balances = ticker['balances']

            re_dict = {}
            if symbol is not None:
                for x in balances:
                    if x['asset'] == symbol:
                        return {symbol: {
                            'free': d(x['free']),
                            'locked': d(x['locked']),
                            'borrowed': d(x.get('borrowed', d(0)))
                        }}

            # 找不到就全送
            for x in balances:
                re_dict.update({x['asset']: {
                    'free': d(x['free']),
                    'locked': d(x['locked']),
                    'borrowed': d(x.get('borrowed', d(0)))
                }})

            return re_dict

        except Exception as e:
            err = readable_error(e, __file__)
            err_str = f"[Orders] _get_account >> {err}\n{ticker}"
            self.logger.error(err_str)
            self.send_mail(f'ERROR {symbol} Binance API', err_str)
            time.sleep(3)
            sys.exit()

    def _get_margin_account(self, symbol) -> dict:
        """
        範例 resp 格式
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
        try:
            assets = self.client.get_margin_account(asset=symbol)

            re_dict = deepcopy(assets)
            del re_dict['userAssets']

            assets = assets['userAssets']

            if symbol is not None:
                for x in assets:
                    if x['asset'] == symbol:
                        re_dict[symbol] = {
                            'free': d(x['free']),
                            'locked': d(x['locked']),
                            'borrowed': d(x.get('borrowed', d(0))),
                            'interest': d(x['interest']),

                        }

            # 找不到就全送
            for x in assets:
                re_dict.update({x['asset']: {
                    'free': d(x['free']),
                    'locked': d(x['locked']),
                    'borrowed': d(x.get('borrowed', d(0))),
                    'interest': d(x['interest']),
                }})

            return re_dict
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] _get_margin_account >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(symbol), err)
            time.sleep(3)
            sys.exit()

    def _get_isolated_margin_account(self, asset, symbols) -> dict:
        """
        特別注意
        這裡的 MarginRatio 就是 leverage ratio for current user
        MarginLevel 就是 小於 ～1.1 就會被 liquidate
        範例 resp 格式
        {
           "assets":[
              {
                "baseAsset":
                {
                  "asset": "BTC",
                  "borrowEnabled": true,
                  "borrowed": "0.00000000",
                  "free": "0.00000000",
                  "interest": "0.00000000",
                  "locked": "0.00000000",
                  "netAsset": "0.00000000",
                  "netAssetOfBtc": "0.00000000",
                  "repayEnabled": true,
                  "totalAsset": "0.00000000"
                },
                "quoteAsset":
                {
                  "asset": "USDT",
                  "borrowEnabled": true,
                  "borrowed": "0.00000000",
                  "free": "0.00000000",
                  "interest": "0.00000000",
                  "locked": "0.00000000",
                  "netAsset": "0.00000000",
                  "netAssetOfBtc": "0.00000000",
                  "repayEnabled": true,
                  "totalAsset": "0.00000000"
                },
                "symbol": "BTCUSDT",
                "isolatedCreated": true,
                "enabled": true, // true-enabled, false-disabled
                "marginLevel": "0.00000000",
                "marginLevelStatus": "EXCESSIVE", // "EXCESSIVE", "NORMAL", "MARGIN_CALL", "PRE_LIQUIDATION", "FORCE_LIQUIDATION"
                "marginRatio": "0.00000000", <--- 這是 leverage ratio for current user
                "indexPrice": "10000.00000000",
                "liquidatePrice": "1000.00000000",
                "liquidateRate": "1.00000000",
                "tradeEnabled": true
              }
            ],
            "totalAssetOfBtc": "0.00000000",
            "totalLiabilityOfBtc": "0.00000000",
            "totalNetAssetOfBtc": "0.00000000"
        }
        """
        try:
            def add_decimal(obj):
                for _key in obj:
                    if _key in ['marginRatio', 'marginLevel', 'indexPrice',
                                'liquidatePrice', 'liquidatePrice', 'liquidateRate']:
                        obj[_key] = d(obj[_key])
                return obj

            if "," in symbols:
                raise Exception(f"Please only use one symbol, but multiple symbols {symbols} found")

            assets = self.client.get_isolated_margin_account(symbols=symbols)

            # 我們一開始就只接受一個symbol
            asset_obj = assets['assets'][0]

            re_dict = deepcopy(asset_obj)
            del re_dict['baseAsset']
            del re_dict['quoteAsset']

            if asset is not None:
                for key in asset_obj:  # iterate through keys
                    if key in ["baseAsset", "quoteAsset"]:
                        x = asset_obj[key]
                        if x['asset'] == asset:
                            re_dict[asset] = {
                                'free': d(x['free']),
                                'locked': d(x['locked']),
                                'borrowed': d(x.get('borrowed', d(0))),
                                'interest': d(x.get('interest', d(0))),
                                'netAsset': d(x.get('netAsset', d(0))),
                                'netAssetOfBtc': d(x.get('netAssetOfBtc', d(0))),
                                'repayEnabled': x.get('netAsset'),
                                'totalAsset': d(x.get('totalAsset', d(0))),
                            }
                            re_dict = add_decimal(re_dict)
                            return re_dict

            # 找不到就全送
            for key in asset_obj:
                if key in ["baseAsset", "quoteAsset"]:
                    x = asset_obj[key]
                    if x['asset'] not in re_dict:
                        re_dict.update({x['asset']: {
                            'free': d(x['free']),
                            'locked': d(x['locked']),
                            'borrowed': d(x.get('borrowed', d(0))),
                            'interest': d(x.get('interest', d(0))),
                            'netAsset': d(x.get('netAsset', d(0))),
                            'netAssetOfBtc': d(x.get('netAssetOfBtc', d(0))),
                            'repayEnabled': x.get('netAsset'),
                            'totalAsset': d(x.get('totalAsset', d(0))),
                        }})

            re_dict = add_decimal(re_dict)
            return re_dict
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] _get_isolated_margin_account >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(asset), err)
            time.sleep(3)
            sys.exit()

    def get_account_balance(self, asset, spot_margin, symbol=None, **kwargs):
        """
        取得 free 的 balance
        請注意：**kwargs是用來跟 DummyOrder 配合來注入取得 c.ds.cash/c.ds.target_cash
        """
        try:
            if spot_margin == 'spot':
                return self._get_account(asset)
            elif spot_margin == 'margin':
                return self._get_margin_account(asset)
            elif spot_margin == 'isolated':
                if symbol is None:
                    raise Exception(f'BinanceOrder.get_account_balance -> expect parameter symbol for isolated account')
                return self._get_isolated_margin_account(asset, symbol)
            else:
                raise Exception(f'BinanceOrder.get_account_balance -> expect spot_margin got {spot_margin}')
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"BinanceOrder.get_account_balance: {err}")
            self.send_mail('ERROR BinanceOrder.get_account_balance', err)

    def get_margin_info(self, asset, spot_margin, symbol=None, **kwargs):
        try:
            if spot_margin == 'spot':
                re_dict = self._get_account(asset)
                re_dict['marginRatio'] = d(1)  # NOT possible
            elif spot_margin == 'margin':
                re_dict = self._get_margin_account(asset)
                re_dict['marginRatio'] = None  # max possible
            elif spot_margin == 'isolated':
                if symbol is None:
                    raise Exception(f'BinanceOrder.get_margin_level -> expect parameter symbol for isolated account')
                re_dict = self._get_isolated_margin_account(asset, symbol)
            else:
                raise Exception(f'BinanceOrder.get_margin_level -> expect margin|isolated got {spot_margin}')

            # del re_dict[asset]

            return re_dict

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"BinanceOrder.get_account_balance: {err}")
            self.send_mail('ERROR BinanceOrder.get_account_balance', err)

    def get_info(self, symbol):
        # get the current file directory
        # cache_dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "cache")
        cache_dir_path = "/tmp"
        file_name = f"{cache_dir_path}/BinanceInfo.pkl"
        found = os.path.isfile(file_name)

        try:
            need_write = False
            if not found:
                info = self.client.get_exchange_info()
                info["my_expiry"] = dt.now() + timedelta(days=3)
                need_write = True
                with open(file_name, 'wb') as fp:
                    pickle.dump(info, fp)

            else:
                # Read dictionary pkl file
                with open(file_name, 'rb') as fp:
                    info = pickle.load(fp)
                    # check expired or not
                    just_passed = dt.now() - timedelta(days=3)
                    if dt.now() > info.get("my_expiry", just_passed):
                        need_write = True

            if need_write:
                info = self.client.get_exchange_info()
                info["my_expiry"] = dt.now() + timedelta(days=3)
                with open(file_name, 'wb') as fp:
                    pickle.dump(info, fp)

            if symbol != "":
                return [market for market in info['symbols'] if market['symbol'] == symbol][0]

            return info

        except Exception as e:
            # remove it once there is an error
            if found:
                os.remove(file_name)
            # logging
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] get_info >> {err}, removing {file_name}")
            self.send_mail('ERROR {0} Binance API'.format(symbol), err)
            sys.exit(3)

    def get_trade_fee(self, symbol) -> dict:
        # get the current file directory
        # cache_dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "cache")
        cache_dir_path = "/tmp"
        file_name = f"{cache_dir_path}/BinanceTradeFee.pkl"
        found = os.path.isfile(file_name)

        try:
            need_write = False
            if not found:
                fee_list = self.client_LIVE.get_trade_fee(symbol=symbol)
                info = {"fee": fee_list, "my_expiry": dt.now() + timedelta(days=1)}
                need_write = True
                with open(file_name, 'wb') as fp:
                    pickle.dump(info, fp)

            else:
                # Read dictionary pkl file
                with open(file_name, 'rb') as fp:
                    info = pickle.load(fp)
                    # check expired or not
                    just_passed = dt.now() - timedelta(days=1)
                    if dt.now() > info.get("my_expiry", just_passed):
                        need_write = True

            if need_write:
                fee_list = self.client_LIVE.get_trade_fee(symbol=symbol)
                info = {"fee": fee_list, "my_expiry": dt.now() + timedelta(days=1)}
                with open(file_name, 'wb') as fp:
                    pickle.dump(info, fp)

            # old simple code
            # if self.exch_mode == "SpotAPI":
            #     info = self.client.get_trade_fee(symbol=symbol)
            # else:
            #     return {'makerCommission': 0.001, 'takerCommission': 0.001}

            if symbol != "":
                if len(info.get("fee")) == 1:
                    return info.get("fee")[0]
                else:
                    raise Exception(f'error get_trade_fee {info}')

            return info.get("fee")

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] get_trade_fee >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(symbol), err)
            time.sleep(3)
            sys.exit()

    def get_spec(self, symbol):
        try:
            if not self.is_validated:
                raise Exception('symbol is not validated')

            if symbol not in self.__spec:
                raise Exception('symbol {0} is not in the validated symbols {1}'.format(symbol, self.__spec.keys()))

            if self.__spec[symbol]['qty_decimal'] is None or self.__spec[symbol]['price_decimal'] is None:
                raise Exception('qty_decimal is {0}, price_decimal is {1}, validation is not done properly'.format(
                    self.__spec[symbol]['qty_decimal'],
                    self.__spec[symbol]['price_decimal']
                ))

            return self.__spec[symbol]
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] get_spec >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(symbol), err)

    @staticmethod
    def remove_exponent(_d):
        return _d.quantize(Decimal(1)) if _d == _d.to_integral() else _d.normalize()

    def set_spec(self, spec):
        self.__spec = spec

    def validate(self, symbols: list, verbose: bool = True):
        try:
            for symbol in symbols:
                data = {'symbol': symbol, 'min_qty': 0, 'min_price': 0, 'min_notional': 0,
                        'step_size': 0, 'tick_size': 0, 'qty_decimal': None, 'price_decimal': None}
                self.__spec[symbol] = data
                self.makerCommission[symbol] = 0.001
                self.takerCommission[symbol] = 0.001

            symbols = list(set(symbols))
            filters = self._filters(symbols)

            # Order book prices
            i = 0
            for symbol in symbols:
                try:
                    a = filters[symbol]['filters']
                    filters[symbol] = {**filters[symbol], **a}
                    filters[symbol].pop("filters")
                    # lastBid, lastAsk, qty1, qty2 = self.get_last_order_book(symbol)
                    # if verbose:
                    #     self.logger.info('[Validator] {0} LastBid, LastAsk: {1}(qty {3}), {2}(qty {4})'
                    #                      ''.format(symbol, lastBid, lastAsk, qty1, qty2))

                    # lastPrice = self.get_ticker(symbol)
                    # if verbose:
                    #     self.logger.info('[Validator] Date: {0}: lastPrice $ {1}'.format(lastPrice[0], lastPrice[1]))
                    """
                    {
                        "symbol":"LTCUSDT",
                        "status":"TRADE_STATUS",
                        "baseAsset":"LTC",
                        "baseAssetPrecision":8,
                        "quoteAsset":"USDT",
                        "quotePrecision":8,
                        "quoteAssetPrecision":8,
                        "baseCommissionPrecision":8,
                        "quoteCommissionPrecision":8,
                        "orderTypes":["LIMIT","LIMIT_MAKER","MARKET","STOP_LOSS_LIMIT","TAKE_PROFIT_LIMIT"],
                        "icebergAllowed":true,"ocoAllowed":true,"quoteOrderQtyMarketAllowed":true,"isSpotTradingAllowed":true,
                        "isMarginTradingAllowed":true,"filters":[
                        {"filterType":"PRICE_FILTER","minPrice":"0.01000000", "maxPrice":"100000.00000000","tickSize":"0.01000000"},
                        {"filterType":"PERCENT_PRICE","multiplierUp":"5","multiplierDown":"0.2","avgPriceMins":5},
                        {"filterType":"LOT_SIZE","minQty":"0.00001000", "maxQty":"90000.00000000","stepSize":"0.00001000"},
                        {"filterType":"MIN_NOTIONAL", "minNotional":"10.00000000","applyToMarket":true,"avgPriceMins":5},
                        {"filterType":"ICEBERG_PARTS","limit":10},
                        {"filterType":"MARKET_LOT_SIZE","minQty":"0.00000000","maxQty":"16020.63002337","stepSize":"0.00000000"},
                        {"filterType":"MAX_NUM_ORDERS","maxNumOrders":200},
                        {"filterType":"MAX_NUM_ALGO_ORDERS","maxNumAlgoOrders":5}],"permissions":["SPOT","MARGIN"]
                    }
                    """
                    min_qty = d(filters[symbol]['LOT_SIZE']['minQty'])
                    self.__spec[symbol]['min_qty'] = d(min_qty)
                    self.__spec[symbol]['min_price'] = d(filters[symbol]['PRICE_FILTER']['minPrice'])
                    self.__spec[symbol]['min_notional'] = d(filters[symbol]['NOTIONAL']['minNotional'])

                    # stepSize defines the intervals that a quantity/icebergQty can be increased/decreased by.
                    step_size = d(filters[symbol]['LOT_SIZE']['stepSize'])
                    # d = self.remove_exponent(decimal.Decimal(str(step_size)))
                    # step_size_decimal = abs(d.as_tuple().exponent)
                    step_size_no_trailing_zero = str(normalize_fraction(str(step_size)))
                    step_size_decimal = int(least_significant_digit_power(step_size_no_trailing_zero))
                    self.__spec[symbol]['step_size'] = step_size
                    self.__spec[symbol]['step_size_decimal'] = step_size_decimal
                    self.__spec[symbol]['fee'] = d(0.1)  # it is fixed for now

                    # tickSize defines the intervals that a price/stopPrice can be increased/decreased by
                    tick_size = d(filters[symbol]['PRICE_FILTER']['tickSize'])
                    # d = decimal.Decimal(str(tick_size))
                    # tick_size_decimal = abs(d.as_tuple().exponent)
                    tick_size_no_trailing_zero = str(normalize_fraction(str(tick_size)))
                    tick_size_decimal = int(least_significant_digit_power(tick_size_no_trailing_zero))
                    self.__spec[symbol]['tick_size'] = tick_size
                    self.__spec[symbol]['tick_size_decimal'] = tick_size_decimal

                    # a = abs(int(f'{min_qty:e}'.split('e')[-1]))
                    # b = len(str(filters[symbol]['LOT_SIZE']['minQty']).split(".")[1])
                    # d1 = max(a, b)

                    self.__spec[symbol]['qty_decimal'] = step_size_decimal

                    # d2 = abs(int(f'{tick_size:e}'.split('e')[-1]))
                    self.__spec[symbol]['price_decimal'] = tick_size_decimal
                    spec_repr = pretty_dict(self.__spec[symbol])

                    self.__spec[symbol]['base_asset'] = filters[symbol]['baseAsset']
                    self.__spec[symbol]['base_asset_precision'] = filters[symbol]['baseAssetPrecision']
                    self.__spec[symbol]['quote_asset'] = filters[symbol]['quoteAsset']
                    self.__spec[symbol]['quote_precision'] = filters[symbol]['quotePrecision']
                    self.__spec[symbol]['quote_asset_precision'] = filters[symbol]['quoteAssetPrecision']
                    self.__spec[symbol]['base_comm_precision'] = filters[symbol]['baseCommissionPrecision']
                    self.__spec[symbol]['quote_comm_precision'] = filters[symbol]['quoteCommissionPrecision']

                    if verbose:
                        self.logger.info(f'[Validator] Spec for Binance {symbol} ==>\n{pformat(filters)}')

                except Exception as e:
                    self.logger.critical(readable_error(e, __file__))
                    sys.exit(1)

                i += 1

            self.__update_fee_commission(symbols, verbose)
            self.is_validated = True

            return True
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] validate >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(symbols), err)

    def __update_fee_commission(self, symbols, verbose=True):
        try:
            for symbol in symbols:
                fee = self.get_trade_fee(symbol)
                if fee is None:
                    raise Exception('func __update_fee_commission cannot be None')
                self.makerCommission[symbol] = fee.get('makerCommission')
                self.takerCommission[symbol] = fee.get('takerCommission')

                if verbose:
                    self.logger.info(f"[Orders] MakerCommission[{symbol}]:{fee.get('makerCommission')}, "
                                     f"takerCommission[{symbol}]:{fee.get('takerCommission')} for Binance")

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] __update_fee_commission >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(symbols), err)

    def _filters(self, symbols):
        try:
            result = {}
            # Get symbol exchange info
            for symbol in symbols:
                symbol_info = self.get_info(symbol)

                # print(symbol_info)

                if not symbol_info:
                    # print('Invalid symbol, please try again...')
                    self.logger.critical('Binance Invalid symbol, please try again...')
                    exit(1)

                symbol_info['filters'] = {item['filterType']: item for item in symbol_info['filters']}
                result[symbol] = symbol_info

            return result
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] _filters >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(symbols), err)

    def get_my_trades(self, symbol: str, limit: int):
        try:
            acct = self.client.get_my_trades(symbol=symbol, limit=limit)
            return acct
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] get_my_trades >> {err}")
            self.send_mail('ERROR {0} Binance API'.format(symbol), err)

    def get_qty_decimal_view(self, symbol) -> int:
        """
        這似乎是用來顯示的，不是用來 trade 的 decimal
        """
        return self.__spec[symbol]['qty_decimal']

    def get_price_decimal_view(self, symbol) -> int:
        """
        這似乎是用來顯示的，不是用來 trade 的 decimal
        """
        return self.__spec[symbol]['price_decimal']

    def get_qty_decimal(self, symbol) -> int:
        """
        用來 trade 的 decimal
        """
        return self.__spec[symbol]['step_size_decimal']

    def get_price_decimal(self, symbol) -> int:
        """
        用來 trade 的 decimal
        """
        return self.__spec[symbol]['price_decimal']  # ['tick_size_decimal']

    def get_precision(self, symbol):
        return {
            "base_asset": self.__spec[symbol]["base_asset"],
            "base_asset_precision": self.__spec[symbol]["base_asset_precision"],
            "quote_asset": self.__spec[symbol]["quote_asset"],
            "quote_precision": self.__spec[symbol]["quote_precision"],
            "quote_asset_precision": self.__spec[symbol]["quote_asset_precision"],
            "base_comm_precision": self.__spec[symbol]["base_comm_precision"],
            "quote_comm_precision": self.__spec[symbol]["quote_comm_precision"]
        }

    def round_price(self, symbol, price) -> Decimal:
        # precision = self.validator.spec[exchange_index]['price_decimal']
        precision = self.__spec[symbol]['tick_size_decimal']
        return d_round(price, precision)  # self.truncate_float

    def round_qty(self, symbol, qty) -> Decimal:
        # precision = self.validator.spec[exchange_index]['qty_decimal']
        precision = self.__spec[symbol]['step_size_decimal']
        return d_round(qty, precision)  # self.truncate_float

    @staticmethod
    def truncate_float(n, places):
        return int(n * (10 ** places)) / 10 ** places


class DummyOrder:
    def __init__(self, exch_mode, logger, **kwargs):
        """
        ==> 看這個或許蠻好的, 也看 上面 def get_margin_requirement_pct
        https://td.intelliresponse.com/tddirectinvesting/public/index.jsp?requestType=NormalRequest&source=4&id=3109&sessionId=fce5e901-c18f-11e8-bee8-63bdcca9f267&question=How+do+I+calculate+the+margin+required+for+a+long+stock+purchase+or+short+sell

        幣安自己的解釋，但需要多思考一下：
        ltv = loan-to-value
        ltv = loan_amt / collateral_amt * 100%
        loan_amount = principal + interest

        數字範例：
        ｜ initial ltv ｜  margin_call ｜ liquidation ｜
        ｜         60% ｜          75% ｜         83% ｜

        ==> 如何理解 margin requirement
        https://td.intelliresponse.com/tddirectinvesting/public/index.jsp?requestType=NormalRequest&source=2&id=8377&uuid=f481eed6-43c8-11ee-9650-79e2e7008ea5&question=How+do+I+maintain+an+adequate+margin+balance
        margin_requirement 把他當作你所以需要的保持的 equity（cash+asset）
        margin_requirement_pct 是百分比，比如 30%
        margin_requirement = current_price * pos * margin_requirement_pct
        然而 margin_used 就是你用掉多少給你的 init_free_margin
        margin_level = equity/margin_used * 100%

        margin_requirement_pct = 1/leverage_ratio
        e.g. 0.2 = 1/5 ==> leverage_ratio = 5

        p.s. 在 Binance, marginRatio 是你現在的 leverage_level
        e.g. def _get_isolated_margin_account 中的 get_isolated_margin_account

        Binance future 的 參照表
        https://www.binance.com/en/support/announcement/updates-on-leverage-and-margin-tiers-of-binance-futures-contracts-2021-08-18-34801a0c405a4b058f9ae18a1a34cad3

        """

        BinanceOrderInject = kwargs.get("BinanceOrderInject")
        if BinanceOrderInject is None:
            raise Exception("You have to inject Binance Order when using Dummy due to training/simulation purpose")
        self.proxy = BinanceOrderInject

        self.exch_mode = exch_mode
        self.logger = logger

        self.__spec = {}
        self.makerCommission = {}
        self.takerCommission = {}

        self.is_validated = False

    def send_mail(self, subject, content):
        try:
            subject = f'{subject} {dt.now()}'
            receivers = self.config.send_mail_receiver
            if receivers:
                receivers = receivers.split(',')
            else:
                receivers = []
            send_mail(receivers, subject, content)
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"[Orders] send_mail >> {err}")

    def validate(self, symbols: list, verbose: bool = True):
        return self.proxy.validate(symbols, verbose)

    @staticmethod
    async def buy_market(symbol, quantity, **kwargs):
        orderId = random.randint(100000000, 900000000)
        executedAmt = quantity * kwargs.get('price')
        order = None
        return orderId, quantity, executedAmt, order

    async def buy_margin_market(self, symbol, quantity, sideEffectType=None, **kwargs):
        orderId = random.randint(100000000, 900000000)
        executedAmt = d_round(quantity * kwargs.get('price'),
                              self.get_precision(symbol).get('quote_asset_precision'))
        order = None
        return orderId, quantity, executedAmt, order

    async def sell_market(self, symbol, quantity, **kwargs):
        orderId = random.randint(100000000, 900000000)
        executedAmt = d_round(quantity * kwargs.get('price'),
                              self.get_precision(symbol).get('quote_asset_precision'))

        order = None
        return orderId, quantity, executedAmt, order

    async def sell_margin_market(self, symbol, quantity, sideEffectType=None, **kwargs):
        orderId = random.randint(100000000, 900000000)
        executedAmt = d_round(quantity * kwargs.get('price'),
                              self.get_precision(symbol).get('quote_asset_precision'))

        order = None
        return orderId, quantity, executedAmt, order

    async def buy_then_repay_market(self, symbol,
                                    buy_qty: Decimal,
                                    repay_asset: str,
                                    repay_qty: Decimal,
                                    loan_interest: Decimal = d(0),
                                    **kwargs):
        repay_qty_interest = repay_qty + loan_interest
        orderId = random.randint(100000000, 900000000)
        executedAmt = d_round(buy_qty * kwargs.get('price'), self.get_precision(symbol).get('quote_asset_precision'))
        order = {
            'executedAmt': executedAmt,
            'executedQty': buy_qty,
            'price_executed': kwargs.get('price'),
            'marginRepayAsset': repay_asset,
            'marginRepayQtyTotal': repay_qty_interest,
            'marginRepayQty': repay_qty,
            'marginRepayInterest': loan_interest,
            'marginRepayTranId': random.randint(1000, 900000)
        }
        return orderId, buy_qty, executedAmt, order

    # async def get_isolated_margin_tier(self, asset, **kwargs):
    #     """
    #
    #     Parameters
    #     ----------
    #     :param asset
    #     :param kwargs:
    #         - :param baseAssetMaxBorrowable: required for AppEnv.TRAIN
    #             used for mocking
    #         - :type float string e.g. "10.1"
    #     """
    #
    #     if kwargs.get('margin_used') is not None:
    #         # 轉換成好像是真實的
    #         re = await self.proxy.get_isolated_margin_tier(asset)
    #         re[0]["baseAssetMaxBorrowable"] = f"{d(re[0].get('baseAssetMaxBorrowable')) - d(kwargs.get('margin_used'))}"
    #     else:
    #         if kwargs.get('effectiveMultiple') is None:
    #             raise Exception("BinanceOrder.get_isolated_margin_tier ==> effectiveMultiple is required")
    #
    #         if kwargs.get('baseAssetMaxBorrowable') is None:
    #             raise Exception("BinanceOrder.get_isolated_margin_tier ==> baseAssetMaxBorrowable is required")
    #
    #         if kwargs.get('quoteAssetMaxBorrowable') is None:
    #             raise Exception("BinanceOrder.get_isolated_margin_tier ==> quoteAssetMaxBorrowable is required")
    #
    #         # re = await self.proxy.get_isolated_margin_tier(symbol)
    #         re = [
    #             {
    #                 "symbol": asset,
    #                 "tier": 1,
    #                 "effectiveMultiple": kwargs.get("effectiveMultiple"),
    #                 "initialRiskRatio": "1.111",
    #                 "liquidationRiskRatio": "1.05",
    #                 "baseAssetMaxBorrowable": str(kwargs.get("baseAssetMaxBorrowable")),
    #                 "quoteAssetMaxBorrowable": str(kwargs.get("quoteAssetMaxBorrowable"))
    #             }
    #         ]
    #
    #     return re

    async def loan_then_short_selling_market(self, symbol, quantity, loan_asset, **kwargs):
        orderId = random.randint(1000000, 900000000)
        executedAmt = d_round(quantity * kwargs.get('price'),
                              self.get_precision(symbol).get('quote_asset_precision'))

        order = None
        return orderId, quantity, executedAmt, order

    @staticmethod
    def get_account_balance(asset, spot_margin, symbol=None, **kwargs):
        """
        取得 free 的 balance
        請注意：
        **kwargs是用來跟 DummyOrder 配合來注入取得 c.ds.cash/c.ds.target_cash

        Parameters
        ----------
        :param asset
        :param spot_margin
        :param symbol

        :param kwargs
            - :param balance
            - :param margin_level
        """
        if kwargs.get('balance') is None:
            raise Exception(f"DummyOrder get_account_balance -> parameter 'balance' is required but got {kwargs}")
        if kwargs.get('margin_level') is None:
            raise Exception(f"DummyOrder get_account_balance -> parameter 'margin_level' is required but got {kwargs}")

        return {
            'marginLevel': d(kwargs.get('margin_level')),
            asset: {
                'free': d(kwargs.get('balance')),
                'locked': d(0.0)
            }
        }

    @staticmethod
    def get_margin_info(asset, spot_margin, symbol=None, **kwargs):
        """
        他其實就是 account_balance 但是就是特別拉出來配合一下
        取得 free 的 balance
        請注意：**kwargs是用來跟 DummyOrder 配合來注入取得 c.ds.cash/c.ds.target_cash

        Parameters
        ----------
        :param asset
        :param spot_margin
        :param symbol

        :param kwargs
            - :param balance
            - :param margin_level
        """
        # return self.get_account_balance(asset, spot_margin, symbol, **kwargs)  # .get(asset)

        if kwargs.get("index_price") is None:
            raise Exception("DummyOrder.get_margin_info requires index_price")

        if kwargs.get("margin_level") is None:
            raise Exception("DummyOrder.get_margin_info requires margin_level")

        if kwargs.get("borrowed") is None:
            raise Exception("DummyOrder.get_margin_info requires borrowed")

        index_price = kwargs.get("index_price")
        margin_level = kwargs.get("margin_level")
        borrowed = kwargs.get("borrowed")

        return {
            "marginLevel": d(margin_level),
            "marginRatio": d(2),  # Binance 系統給的, 故意先設 arbitrary 2
            "indexPrice": d(index_price),
            asset: {
                "borrowed": borrowed
            }
        }

    def get_decimal_price_qty(self, symbol) -> dict:
        return self.proxy.get_decimal_price_qty(symbol)

    def get_precision(self, symbol) -> dict:
        return self.proxy.get_precision(symbol)

    def get_spec(self, symbol) -> dict:
        return self.proxy.get_spec(symbol)

