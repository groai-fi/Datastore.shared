import sys
import time

import asyncio
from os import path
import numpy as np
import pandas as pd
import datetime as dt
from time import sleep
from .enums import TradeAction, ErrorCodes, AppEnv
from .BaseBacktestOrder import BaseBacktestOrder
from decimal import Decimal
from .utils import readable_error, save_data, d_round, d, d_is_close, d_negate, d_abs, d_round_fee
from .utils import set_reset_trade_cash, get_reset_trade_cash_txt, get_project_root, return_not_matches


project_root = get_project_root()


class BacktestOrder(BaseBacktestOrder):
    def __init__(self, data_store, exch_order, trade_args, app_env, logger, spot_margin=None, **kwargs):

        if exch_order is None:
            min_qty = d(1)
            min_notional = d(10)
        else:
            min_qty = exch_order.get_spec(trade_args["symbol"])["min_qty"]
            min_notional = exch_order.get_spec(trade_args["symbol"])["min_notional"]

        logger.info(f"[BasicTrade] Setting min_qty = {min_qty}, min_notional = {min_notional}")
        super().__init__(data_store, exch_order, app_env, min_notional, min_qty, logger)
        self.trade_args = trade_args
        # self.set_decimal_price_qty()
        self.symbol = self.trade_args.get("symbol")
        self.target_asset = self.trade_args.get("target_asset")
        self.home_asset = self.symbol.replace(self.target_asset, "")
        self.spot_margin = trade_args.get("spot_margin") if spot_margin is None else spot_margin

        if self.spot_margin not in ["spot", "margin", "isolated"]:  # "margin",
            raise Exception("spot_margin can only be spot or margin or isolated")

        self.pause = 3

        # margin_requirement_pct is a must for margin account and isolated account
        self.margin_info = self.exch_order.get_margin_info(
            self.ds.target_asset,
            self.spot_margin,
            self.symbol,
            index_price=d(0),
            margin_level=d(999),
            borrowed=d(0)
        )

        self.sys_leverage_ratio = self.margin_info.get("marginRatio")

        # 我們只用 leverage_ratio 比較小的
        self.user_leverage_ratio = min(d(trade_args.get("leverage_ratio", 1)), self.sys_leverage_ratio)
        # 計算出來這關係給你看而已，用不到了
        # self.margin_requirement_pct = d(1) / self.user_leverage_ratio

        # TODO 少過這個值後，無法再借貸，看以後是否可以變成自動帶入
        self.margin_call_level = d(trade_args.get("margin_call_level"))
        self.max_margin_qty = d(trade_args.get("max_margin_qty"))
        self.logger.info(f"[BacktestOrder] margin_call_level={self.margin_call_level}, "
                         f"max_margin_qty={self.max_margin_qty}")

        user_id = self.trade_args.get("user_id")
        bot_id = self.trade_args.get("bot_id")
        assert (user_id is not None, "BacktestOrder.user_id cannot be None")
        assert (bot_id is not None, "BacktestOrder.bot_id cannot be None")
        self.user_bot_id = f"{user_id}.{bot_id}"

    def hold(self, idx: int, dt_idx: dt.datetime, price: Decimal,
             trade_hold_code: ErrorCodes = None):
        """
        get_current_row()
            "dt_idx": self.dt_idx[idx],
            "idx_trade": idx,
            .... 已處理 ...
            "kelly_cap": round(self._kelly_cap[idx], 3),
            "kelly_cap_short": round(self._kelly_cap_short[idx], 8),
            "silo_pos": silo_pos_str,
            "silo_amt": silo_amt_str,
            "silo_short_pos": silo_short_pos_str,
            "silo_short_amt": silo_short_amt_str,

            # extra non-essential fields
            "cumulative_realized_pnl": self._cumulative_realized_pnl[idx],
            "drawdown": self._drawdown[idx],

        """
        try:
            if not isinstance(price, Decimal):
                raise Exception(f"hold()->price can only be type Decimal but got {type(price)}")

            # 更新價格
            self.ds.set_price(dt_idx, price, self.app_env, True)

            # fee
            self.ds.set_fee1(d(0))
            self.ds.set_fee2(d(0))

            # buysell_lvl, 如果外部沒設定才會執行
            # TODO, missing at lower level
            # if self.ds.get_buysell_lvl() is None:
            #    self.ds.set_buysell_lvl(self.ds.get_last_buysell_lvl())

            # set_order_id
            self.ds.set_order_id(None, d(0), d(0))

            # set_trade
            last_position = self.ds.get_last_position()
            last_cash = self.ds.get_last_cash()
            last_borrowed_cash = self.ds.get_last_borrowed_cash()
            asset = self.ds.silo.get_total_pos() * price
            self.ds.set_trade(TradeAction.HOLD, last_position, last_cash, last_borrowed_cash, asset)

            # trade_cash
            self.ds.set_trade_cash(self.ds.get_last_trade_cash())

            # target_cash
            target_cash = self.ds.get_last_target_cash()
            if self.app_env == AppEnv.TRADE:
                target_cash = self.get_target_balance()
            self.ds.set_target_cash(d(target_cash))

            # update silos
            # 沒有交易所以不用

            # total_amt
            total_pos = self.ds.silo.get_total_pos()
            total_amt = self.ds.silo.get_total_amt_bought()
            paper_pnl = asset - total_amt
            paper_pnl_pct = d_round(paper_pnl / d_abs(total_amt), 5) if d_abs(total_amt) > 0 else d(0.)
            self.ds.set_total(paper_pnl, paper_pnl_pct, total_pos, total_amt)

            # drawdown
            self.cal_drawdown()

            # pnl
            self._copy_last_cumulative_realized_pnl()
            return {
                "trade_action": TradeAction.HOLD,
                "executedAmt": d(0),
                "executedQty": d(0),
                "fee": d(0),
                "fee_asset": self.target_asset,
                "realized_pnl": d(0),
                "realized_pnl_pct": d(0),
                "error_code": trade_hold_code
            }

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(err)
            time.sleep(3)
            sys.exit()

    def buy(self,
            idx: int,
            dt_idx: dt.datetime,
            action: Decimal,
            price: Decimal):
        try:
            if not isinstance(price, Decimal):
                raise Exception(f"buy()->price can only be type Decimal but got {type(price)}")

            if not isinstance(action, Decimal):
                raise Exception(f"action can only be type Decimal but got {type(action)}")

            # 如果已經買幾乎超過 trade_cash 就不要再買了
            last_asset = self.ds.get_last_asset()
            last_position = self.ds.get_last_position()
            last_trade_cash = self.ds.get_last_trade_cash()
            if last_asset * d(1.1) > last_trade_cash:
                self.logger.debug(f"[BacktestOrder] ERROR_TH011 asset {last_asset} * 1.1 is larger than"
                                  f"trade_cash {last_trade_cash}")
                self.logger.info("[BacktestOrder] ERROR_TH011 Changing BUY to HOLD due to trade_cash")

                return self.hold(idx, dt_idx, price, trade_hold_code=ErrorCodes.ERROR_TH011)
            # [end if]

            # 更新價格
            self.ds.set_price(dt_idx, price, self.app_env, True)

            # 計算交易 qty, 避免錢中途改變，檢查一下，只選擇比較少的
            trade_cash = self.ds.get_last_trade_cash() if self.app_env == AppEnv.TRADE else d("Inf")
            cash_available = min(self.ds.get_last_cash(), trade_cash)

            #                      self.get_home_balance(balance=self.ds.get_last_cash(),
            #                                           margin_level=d(999)))

            buy_num_total = cash_available * d(0.98) / (price * (d(1) + self.ds.buy_cost_rate))
            _buy_num = buy_num_total * action

            # buy_num = min(buy_num_total, action * buy_num_total)
            buy_num = d_round(_buy_num, self.ds.decimal_qty)

            # min qty check
            buy_num = self.min_qty * d(2) if buy_num < self.min_qty else buy_num


            self.logger.debug(f"[BacktestOrder] def buy 1:\n"
                             f"buy_num_total={round(buy_num_total, 10)}, "
                             f"cash={self.ds.get_last_cash()},"
                             f"action={action},"
                             f"_buy_num={round(_buy_num, 10)},"
                             f"buy_num={buy_num},"
                             f"min_qty={self.min_qty},"
                             f"min_notional={self.min_notional},"
                             f"amount={price * buy_num},"
                             )

            # cannot be less than self.min_notional.
            amount = price * buy_num
            while amount < self.min_notional:
                buy_num = self.min_notional * d(1.03) / price
                amount = buy_num * price
                self.logger.debug(f"[BacktestOrder] def buy 2:\n"
                                  f"buy_num={buy_num:.8f}, amount={amount:.8f}")
                buy_num = d_round(buy_num, self.ds.decimal_qty)
                if buy_num == d(0.0):
                    buy_num = d(1 * 10 ** (-self.ds.decimal_qty))
                    amount = buy_num * price
                self.logger.debug(f"[BacktestOrder] def buy 3:\nbuy_num={buy_num:.8f}")

            """
            if price * buy_num < 10:
                buy_num = 11 / price
                buy_num = round(buy_num, self.ds.decimal_qty)
                if buy_num == 0.:
                    buy_num = 1 * 10 ** (-self.ds.decimal_qty)
            """

            # not enough money, hold
            if buy_num * price > self.ds.get_last_cash() * d(0.98):
                self.logger.warning(f"[BacktestOrder] ERROR_TH005 Buy failed due to short of cash got {self.ds.get_last_cash()}")
                return self.hold(idx, dt_idx, price, trade_hold_code=ErrorCodes.ERROR_TH005)
            # [end if]

            # ======= START client execute ========
            if self.spot_margin == "spot":
                orderId, executedQty, executedAmt, order = asyncio.run(
                    self.exch_order.buy_market(self.ds.symbol, buy_num, price=price))
            else:
                orderId, executedQty, executedAmt, order = asyncio.run(
                    self.exch_order.buy_margin_market(self.ds.symbol, buy_num, price=price))

            re = {"executedQty": executedQty, "executedAmt": executedAmt,
                  "orderId": orderId, "order": order}

            executedAmt = re.get("executedAmt")
            executedQty = receivedQty = re.get("executedQty")
            fee = d(0.0)
            if self.ds.exch_mode == "SpotAPI":
                if order and order.get("fills"):
                    fee = d_round_fee(sum([d(item.get("commission")) for item in order.get("fills")]),
                                      self.ds.quote_comm_precision)
                else:
                    """
                    只用在 TRAIN: 不夠準確，不要自己算，用他的沒關係，因為最重要的是 target_cash 和 cash
                    """
                    fee = d_round_fee(buy_num * self.ds.buy_cost_rate, self.ds.quote_comm_precision)
                receivedQty = d_round(buy_num - fee, self.ds.quote_comm_precision)

            orderId = re.get("orderId")

            # 更新價格
            price_executed_exact = executedAmt / executedQty
            price_executed = d_round(price_executed_exact, self.ds.decimal_price)
            self.ds.set_price(dt_idx, price_executed, self.app_env, True)

            # fee
            self.ds.set_fee1(fee)
            self.ds.set_fee2(d(0))

            # set_order_id
            self.ds.set_order_id(orderId, executedQty, executedAmt)
            # ======= END client execute ========

            # set_trade
            last_cash = self.ds.get_last_cash()
            last_borrowed_cash = self.ds.get_last_borrowed_cash()
            cash_exact = last_cash - executedAmt  # 扣掉，因為已支出 + 放到 silo 最後去扣掉，因為需要計算正確 realized_pnl
            cash = d_round(cash_exact, self.ds.quote_precision)

            new_position = d_abs(last_position + receivedQty)
            new_asset = d_round(new_position * price_executed, self.ds.quote_precision)
            # new_asset = self.ds.get_asset(idx - 1) + executedAmt
            new_position = d_round(new_position, self.ds.base_asset_precision)
            self.ds.set_trade(TradeAction.BUY, new_position, cash, last_borrowed_cash, new_asset)

            # trade_cash
            self.ds.set_trade_cash(self.ds.get_last_trade_cash())

            # target_cash
            target_cash_new_expect = target_cash_new = self.ds.get_last_target_cash() + receivedQty
            if self.app_env == AppEnv.TRADE:
                target_cash_new = self.get_target_balance()
            if target_cash_new_expect != target_cash_new:
                self.logger.warning(f"BacktestOrder.buy target_cash out of sync ==>\n"
                                    f"actual {target_cash_new} calculated {target_cash_new_expect}")
            self.ds.set_target_cash(target_cash_new)

            # update silos
            # 我們紀錄實際拿到的，但是 amt 是總數，這樣容易計算成本。
            self.ds.silo.record_buy(receivedQty, executedAmt, self.ds.is_significant_pos())  # 本來用 cost

            # total_amt
            total_pos = self.ds.silo.get_total_pos()
            total_amt = self.ds.silo.get_total_amt_bought()

            paper_pnl = new_asset - total_amt
            paper_pnl_pct = d_round(paper_pnl / d_abs(total_amt), 5) if d_abs(total_amt) > 0 else d(0.)
            self.ds.set_total(paper_pnl, paper_pnl_pct, total_pos, total_amt)

            if not d_is_close(new_position, total_pos, self.ds.base_asset_precision):
                raise Exception(f"silo positions {total_pos} differs from position {new_position} when buy")

            # drawdown
            self.cal_drawdown()

            # pnl
            cumulative_realized_pnl = self.ds.get_last_cumulative_realized_pnl()
            realized_pnl, realized_pnl_pct = d(0.0), d(0.0)
            self.ds.set_pnl(realized_pnl, realized_pnl_pct, cumulative_realized_pnl)

            data = dict({
                "trade_action": TradeAction.BUY,
                "price_executed": price_executed,
                "receivedQty": receivedQty,
                "fee": fee,
                "fee_asset": self.target_asset,
                "realized_pnl": realized_pnl,
                "realized_pnl_pct": d_round(realized_pnl_pct, 5),
                "error_code": None
            }, **re)
            return data

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(err)
            time.sleep(3)
            sys.exit()

    def sell(self, idx: int, dt_idx: dt.datetime, action: Decimal, price: Decimal, debug=False):
        try:
            if not isinstance(price, Decimal):
                raise Exception(f"sell()->price can only be type Decimal but got {type(price)}")

            if not isinstance(action, Decimal):
                raise Exception(f"action can only be type Decimal but got {type(action)}")

            # 更新價格
            self.ds.set_price(dt_idx, price, self.app_env, True)

            # 計算交易 qty
            total_position = self.ds.silo.get_total_pos()

            if total_position == 0:
                raise Exception("position is zero when you are trying to sell")

            # sell_num_exact = min(total_position * d(1.5) * d_abs(action + d(0.4)), total_position)
            sell_num_exact = d_abs(action * total_position)
            sell_num = d_round(sell_num_exact, self.ds.decimal_qty)
            remain_qty = total_position - sell_num

            # 檢查賣的量是否太低
            if sell_num * price <= self.min_notional * d(1.2):
                # 不用擔心 sell_num 超過 pos, 因為 remain_qty 會變負數，會被"檢查剩餘的量" 攔截到
                sell_num = d_round(self.min_notional * d(1.2) / price, self.ds.decimal_qty)
                remain_qty = total_position - sell_num

            # 檢查剩餘的量
            if remain_qty * price <= self.min_notional * d(2) or \
                    remain_qty <= self.min_qty * d(2):
                # 這不是寫錯，買買的小數點少於你可以持有的，所以其實你無法全賣
                sell_num_exact = total_position
                self.logger.debug(f">> remain_qty less than 2 times ==>\n"
                                  f"min_notional requires {self.min_notional} and min_qty requires {self.min_qty} "
                                  f"try to expand qty from {sell_num} to {total_position}")
                sell_num = d_round(sell_num_exact, self.ds.decimal_qty)

            if sell_num * price < self.min_notional:
                # 這不是寫錯，全賣還是少於必要條件，只能 hold
                self.logger.debug(f"[BacktestOrder] ERROR_TH004 min_notional requires {self.min_notional} got {sell_num * price} ==> "
                                  f"CONVERT to hold")
                return self.hold(idx, dt_idx, price, trade_hold_code=ErrorCodes.ERROR_TH004)
            # [end if]

            if sell_num <= self.min_qty:
                # 這不是寫錯，全賣還是少於必要條件，只能 hold
                self.logger.debug(f"[BacktestOrder] ERROR_TH002 min_qty requires {self.min_qty} try to sell {sell_num}\n"
                                  f"CONVERT to hold")
                return self.hold(idx, dt_idx, price, trade_hold_code=ErrorCodes.ERROR_TH002)
            # [end if]

            # ======= Start client execute ========
            if self.spot_margin == "spot":
                orderId, executedQty, executedAmt, order = asyncio.run(
                    self.exch_order.sell_market(self.ds.symbol, sell_num, price=price))
            else:
                orderId, executedQty, executedAmt, order = asyncio.run(
                    self.exch_order.sell_margin_market(self.ds.symbol, sell_num, price=price))

            re = {"executedQty": executedQty, "executedAmt": executedAmt,
                  "orderId": orderId, "order": order}

            executedAmt = receivedAmt = re["executedAmt"]
            executedQty = receivedQty = re["executedQty"]
            fee = d(0)
            if self.ds.exch_mode == "SpotAPI":
                """
                {"symbol": "ETHUSDT", "orderId": 14048337745, "clientOrderId": "wVfRUQBtyCOlVhU00JLoW2", 
                "transactTime": 1692713101617, "price": "0", "origQty": "0.3439", "executedQty": "0.3439", 
                "cummulativeQuoteQty": "570.380354", "status": "FILLED", "timeInForce": "GTC", "type": "MARKET", "side": "SELL", 
                "fills": [
                    {"price": "1658.66", "qty": "0.1131", "commission": "0.18759445", "commissionAsset": "USDT", "tradeId": 1189253233}, 
                    {"price": "1658.64", "qty": "0.01", "commission": "0.0165864", "commissionAsset": "USDT", "tradeId": 1189253234}, 
                    {"price": "1658.56", "qty": "0.01", "commission": "0.0165856", "commissionAsset": "USDT", "tradeId": 1189253235}, 
                    {"price": "1658.51", "qty": "0.2108", "commission": "0.34961391", "commissionAsset": "USDT", "tradeId": 1189253236}
                ], "isIsolated": False, "selfTradePreventionMode": "NONE"}
                
                直接像下一行這樣計算可以，但會造成極小誤差，因為是各別 round 後加總，所以跟直接 round 有差別
                DEPRECATED: 不夠準確，不要自己算，用他的沒關係，因為最重要的是 target_cash 和 cash
                """

                if order and order.get("fills"):
                    fee = d_round_fee(sum([d(item.get("commission")) for item in order.get("fills")]),
                                      self.ds.base_comm_precision)
                else:
                    """
                    只用在 TRAIN: 不夠準確，不要自己算，用他的沒關係，因為最重要的是 target_cash 和 cash
                    """
                    fee = d_round_fee(executedAmt * self.ds.sell_cost_rate, self.ds.base_comm_precision)

                receivedQty = receivedAmt = d_round(executedAmt - fee, self.ds.base_asset_precision)
            orderId = re["orderId"]
            decimal_amt = len(str(executedAmt).split(".")[1])

            # 更新價格
            price_executed_exact = executedAmt / executedQty
            price_executed = d_round(price_executed_exact, self.ds.decimal_price)
            self.ds.set_price(dt_idx, price_executed, self.app_env, True)

            # fee
            self.ds.set_fee1(d(0))
            self.ds.set_fee2(fee)

            # set_order_id
            self.ds.set_order_id(orderId, executedQty, executedAmt)

            # append trade history, shrink trade_cash if fail many times
            # TODO: adjust trade_cash if it is too low
            # self.trade_cash = 15 if self.trade_cash < 15 else self.trade_cash

            # ======= END client execute ========

            # set_trade
            last_position = self.ds.get_last_position()
            last_cash = self.ds.get_last_cash()
            last_borrowed_cash = self.ds.get_last_borrowed_cash()
            cash = d_round(last_cash + receivedAmt, self.ds.quote_precision)

            # trade_cash
            self.ds.set_trade_cash(self.ds.get_last_trade_cash())

            # if d_is_close(last_position, sell_num, self.ds.quote_asset_precision):
            #    new_position = d_round(0.0, self.ds.quote_asset_precision)
            # else:
            new_position = d_round(last_position - sell_num, self.ds.base_asset_precision)
            new_asset = d_round(new_position * price_executed, self.ds.quote_precision)
            self.ds.set_trade(TradeAction.SELL, new_position, cash, last_borrowed_cash, new_asset)

            # target_cash
            last_target_cash = self.ds.get_last_target_cash()
            target_cash_new = last_target_cash - executedQty
            self.ds.set_target_cash(target_cash_new)

            # update silos
            # We do not use these two lines anymore, we use executedAmt straight away
            # amt_sold = price_executed * sell_num

            if debug and False:
                self.logger.info(">> record_sell check >>\n"
                                 f"{'idx':>10}"
                                 f" | {'position_prior':>15}"
                                 f" | {'position_post':>14}"
                                 f" | {'sell_num':>15}"
                                 f" | {'executedQty':>15}\n"
                                 f"{idx:>10}"
                                 f" | {last_position:>15}"
                                 f" | {new_position:>15}"
                                 f" | {sell_num:>15}"
                                 f" | {executedQty:>15}"
                                 )
            realized_pnl, realized_pnl_pct = self.ds.silo.record_sell(sell_num,
                                                                      receivedAmt,
                                                                      new_position,
                                                                      self.ds.decimal_qty)
            # total_amt
            total_pos = self.ds.silo.get_total_pos()
            total_amt = self.ds.silo.get_total_amt_bought()
            paper_pnl = new_asset - total_amt
            paper_pnl_pct = d_round(paper_pnl / d_abs(total_amt), 5) if d_abs(total_amt) > 0 else d(0.)
            self.ds.set_total(paper_pnl, paper_pnl_pct, total_pos, total_amt)

            if not d_is_close(new_position, total_pos, self.ds.base_asset_precision):
                raise Exception(f"sell() silo positions {total_pos} differs from position {new_position} when sell")

            # drawdown
            self.cal_drawdown()

            # pnl
            realized_pnl_pct = realized_pnl_pct
            cumulative_realized_pnl = self.ds.get_last_cumulative_realized_pnl() + realized_pnl
            self.ds.set_pnl(realized_pnl, realized_pnl_pct, cumulative_realized_pnl)

            data = dict({
                "trade_action": TradeAction.SELL,
                "price_executed": price_executed,
                "receivedQty": receivedQty,
                "fee": fee,
                "fee_asset": self.home_asset,
                "decimal_amt": decimal_amt,
                "realized_pnl": realized_pnl,
                "realized_pnl_pct": d_round(realized_pnl_pct, 5),
                "error_code": None
            }, **re)

            # 增加交易次數
            self.ds.inc_num_trades()
            self.ds.append_profit_trade(realized_pnl > d(0))

            return data
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(err)
            time.sleep(3)
            sys.exit()

    def cover(self, idx: int, dt_idx: dt.datetime, action: Decimal, price: Decimal, debug=False):
        """
        特別注意這裡，在 margin account 做交易時，你 buy to cover 100 個原本買的 BTC (home_asset USDT, aka quote asset),
        你的手續費必須用 BTC 支付，你不能只買回 100 BTC，你最好買 100.1 (0.1% commission)
        如果你只買 100 個，會出現手續費變成你還是欠他 short margin position (需要付借款利息）
        而且這樣做有個好處，你把 fee_cost 加進去買，你馬上知道是否這交易賺錢，準確很多。
        p.s. fee_cost 是我們多買，來代替 cover 的買回 手續費成本
        但是不管你買什麼數量，0.1 是 fee, 而 fee_of_fee 是 0.1 的千分之一
        所以不管如何都會產生 fee_of_fee, 所以你必須有一點點 target_asset。
        但是 fee_of_fee 很小，假設買賣100萬台幣，它的千分之一的千分之一(fee_of_fee)只有1元。
        所以記得 cover 時記得買 cover_num * (1 + cover_commission)

        Notice1：
        我們利息另外算
        """
        # 為了 catch except 設置
        total_position_prior = 0
        silo_position_str_prior = self.ds.silo.get_position_str()
        try:
            if not isinstance(price, Decimal):
                raise Exception(f"cover()->price can only be type Decimal but got {type(price)}")

            if not isinstance(action, Decimal):
                raise Exception(f"action can only be type Decimal but got {type(action)}")

            # 更新價格
            self.ds.set_price(dt_idx, price, self.app_env, True)

            # 計算交易 qty
            total_position = total_position_prior = self.ds.silo.get_total_pos()

            if total_position == 0:
                raise Exception("position is zero when you are trying to cover")

            cover_num_positive_exact = d_abs(action * total_position)
            cover_num_positive = d_round(cover_num_positive_exact, self.ds.decimal_qty)
            # fee_cost 是我們多買，來代替 cover 的買回 手續費成本
            fee_cost = d_round_fee(cover_num_positive * self.ds.cover_cost_rate, self.ds.base_comm_precision)
            repay_qty = d_round(cover_num_positive, self.ds.base_asset_precision)
            remain_qty = total_position + cover_num_positive

            # cover all if a little bit left
            if (d_abs(remain_qty * price) < self.min_notional * d(2) or
                    cover_num_positive < self.min_qty * d(2) or
                    cover_num_positive * price < self.min_notional):
                cover_num_positive_exact = d_abs(total_position)
                cover_num_positive = d_round(cover_num_positive_exact, self.ds.decimal_qty)
                fee_cost = d_round_fee(cover_num_positive * self.ds.cover_cost_rate, self.ds.base_comm_precision)
                repay_qty = d_round(cover_num_positive, self.ds.base_asset_precision)
                # fee_of_fee = d_round_fee(fee * self.ds.cover_cost_rate, self.ds.base_comm_precision)

            # 很重要，請看 function 說明

            # TODO, 這裡與交易這麼辦？如果買部分，該這麼辦？
            cover_num_positive_fee = d_round(cover_num_positive + fee_cost,
                                             self.ds.decimal_qty)  # rounding=ROUND_UP

            # 我們利息另外償清

            # ======= START client execute ========
            orderId, executedQty, executedAmt, order = asyncio.run(
                self.exch_order.buy_then_repay_market(self.ds.symbol,
                                                      cover_num_positive_fee,
                                                      self.target_asset,
                                                      repay_qty,
                                                      loan_interest=d(0),
                                                      price=price))
            re = {"executedQty": executedQty, "executedAmt": executedAmt, "orderId": orderId,
                  "marginRepayAsset": order.get("marginRepayAsset"),
                  "marginRepayQtyTotal": order.get("marginRepayQtyTotal"),
                  "marginRepayQty": order.get("marginRepayQty"),
                  "marginRepayTranId": order.get("marginRepayTranId")
                  }

            executedAmt = re["executedAmt"]
            executedQty = receivedQty = re["executedQty"]
            marginRepayAsset = re["marginRepayAsset"]  # 給你看得而已
            marginRepayQty = re["marginRepayQty"]  # 給你看得而已
            marginRepayTranId = re["marginRepayTranId"]  # 給你看得而已

            if order and order.get("fills"):
                fee = d_round_fee(sum([d(item.get("commission")) for item in order.get("fills", [])]),
                                  self.ds.base_comm_precision)
            else:
                """
                只用在 TRAIN: 不夠準確，不要自己算，用他的沒關係，因為最重要的是 target_cash 和 cash
                """
                #TODO 是應該 marginRepayQty 還是 executedQty?
                # fee = d_round_fee(d_abs(marginRepayQty) * self.ds.cover_cost_rate,
                #                   self.ds.base_comm_precision)
                fee = d_round_fee(d_abs(executedQty) * self.ds.cover_cost_rate,
                                self.ds.base_comm_precision)

            orderId = re.get("orderId")
            decimal_amt = len(str(executedAmt).split(".")[1])  # sanity check 而已
            assert decimal_amt <= self.ds.quote_asset_precision, \
                f"receivedQty decimal expect {decimal_amt} " \
                f"got {self.ds.quote_asset_precision}"

            # 更新價格
            price_executed_exact = executedAmt / executedQty
            price_executed = d_round(price_executed_exact, self.ds.decimal_price)
            self.ds.set_price(dt_idx, price_executed, self.app_env, True)

            # fee
            self.ds.set_fee1(d(0))
            self.ds.set_fee2(fee)

            # set_order_id
            self.ds.set_order_id(orderId, executedQty, executedAmt)

            # ======= END client execute ========

            # set_trade
            last_position = self.ds.get_last_position()
            last_cash = self.ds.get_last_cash()
            cash_new = last_cash - executedAmt
            self.logger.debug(f"[BacktestOrder] new_cash:{cash_new} = last_cash ( {last_cash} ) - executedAmt ( {executedAmt} )")

            if d_is_close(d_abs(last_position), cover_num_positive, self.ds.base_asset_precision):
                new_position = d_round(0.0, self.ds.base_asset_precision)
            else:
                new_position = last_position + cover_num_positive
                self.logger.debug(f"[BacktestOrder] new_position:{new_position} = "
                                  f"last_position ( {last_position} ) + cover_num_positive ( {cover_num_positive} )")
            new_asset = d_round(new_position * price_executed, self.ds.quote_precision)
            self.logger.debug(f"[BacktestOrder]new_asset {new_asset} ="
                              f" new_position ( {new_position} ) x price_executed ( {price_executed} )")

            # trade_cash
            self.ds.set_trade_cash(self.ds.get_last_trade_cash())

            # target_cash
            last_target_cash = self.ds.get_last_target_cash()
            target_cash_expect = target_cash_now = last_target_cash + executedQty - fee - repay_qty

            if self.app_env == AppEnv.TRADE:
                i = 0
                while target_cash_expect != target_cash_now and i < 10:
                    sleep(self.pause * 3)
                    target_cash_now = self.get_target_balance()
                    i += 1
                    if i >= 10:
                        self.logger.error(f">> target_cash_expect: {target_cash_expect}, "
                                          f"target_cash_now: {target_cash_now}")

            self.logger.debug(f">> last_target_cash: {last_target_cash}, target_cash balance: {target_cash_now}, trading min_qty: {self.min_qty}")

            if target_cash_expect != target_cash_now:
                self.logger.warning(f">> target_cash_expect {target_cash_expect} but got actual {target_cash_now}")

            self.ds.set_target_cash(target_cash_expect)

            # update silos
            realized_pnl, realized_pnl_pct = self.ds.silo.record_cover(cover_num_positive, executedAmt,
                                                                       self.ds.decimal_qty, new_position)
            # total_amt
            silo_total_pos = self.ds.silo.get_total_pos()
            total_amt_bought = self.ds.silo.get_total_amt_bought()

            # total_amt_bought is negative and new_asset is negative
            # TODO: abs(-550) - abs(-500) = +50 賺50元
            paper_pnl = d_abs(total_amt_bought) - d_abs(new_asset)
            paper_pnl_pct = d_round(paper_pnl / d_abs(total_amt_bought), 5) if d_abs(total_amt_bought) > 0 else d(0.)
            self.ds.set_total(paper_pnl, paper_pnl_pct, silo_total_pos, total_amt_bought)

            if not d_is_close(new_position, silo_total_pos, self.ds.base_asset_precision):
                raise Exception(f"silo positions {silo_total_pos} differs from position {new_position} when cover")

            # drawdown
            self.cal_drawdown()

            # pnl
            realized_pnl = d_round(realized_pnl, self.ds.quote_precision)
            realized_pnl_pct = d_round(realized_pnl_pct, self.ds.quote_precision)
            cumulative_realized_pnl = self.ds.get_last_cumulative_realized_pnl() + realized_pnl
            new_borrowed_cash_positive = self.ds.get_last_borrowed_cash() - marginRepayQty  # positive
            self.ds.set_trade(TradeAction.COVER, new_position, cash_new,
                              new_borrowed_cash_positive, new_asset)
            self.ds.set_pnl(realized_pnl, realized_pnl_pct, cumulative_realized_pnl)

            # re 有包括以下欄位
            # "marginRepayAsset": marginRepayAsset,
            # "marginRepayQty": marginRepayQty,
            # "marginRepayTranId": marginRepayTranId
            data = dict({
                "trade_action": TradeAction.COVER,
                "price_executed": price_executed,
                "receivedQty": receivedQty,
                "fee": fee,
                "fee_asset": self.home_asset,
                "decimal_amt": decimal_amt,
                "realized_pnl": realized_pnl,
                "realized_pnl_pct": d_round(realized_pnl_pct, 5),
                "error_code": None
            }, **re)

            # 增加交易次數
            self.ds.inc_num_trades()
            self.ds.append_profit_trade(realized_pnl > d(0))

            return data

        except Exception as e:
            last_position = self.ds.get_last_position()
            new_position = self.ds.get_position()
            cover_num_positive = d_abs(action * total_position_prior)

            err = readable_error(e, __file__)
            err_str = f"{err} silo {silo_position_str_prior}, " \
                      f"last_position: {last_position}, " \
                      f"new_position: {new_position}, " \
                      f"cover_num_positive: {cover_num_positive}, " \
                      f"silo total_position_prior: {total_position_prior}"
            self.logger.error(err_str)
            sys.exit()

    def short(self, idx: int, dt_idx: dt.datetime, action: Decimal, price: Decimal, debug=False):
        """
        特別注意這裡，在 margin account 做交易時，你short 100個 BTC,
        會得到 100個 BTC （借來的），手續費是用 quote_asset(e.g.USDT)
        """
        try:
            if not isinstance(price, Decimal):
                raise Exception(f"short()->price can only be type Decimal but got {type(price)}")

            if not isinstance(action, Decimal):
                raise Exception(f"action can only be type Decimal but got {type(action)}")

            # 更新價格
            self.ds.set_price(dt_idx, price, self.app_env, True)

            # 計算交易 qty
            last_cash = self.ds.get_last_cash()
            short_num_total = last_cash / price * d(0.8)
            short_num_positive_exact = short_num_total * action

            short_num_positive = _short_num_positive = d_round(short_num_positive_exact, self.ds.decimal_qty)

            # min qty check
            short_num_positive = self.min_qty * d(2) if short_num_positive < self.min_qty * d(2) else short_num_positive

            # not enough money, hold
            short_amt_positive = short_num_positive * price
            # min notional check
            if short_amt_positive <= self.min_notional * d(2):
                self.logger.warning(f"[BinanceTrade] ERROR_TH006 Short failed due to short of cash, "
                                    f"min_notional {self.min_notional} x 1.5 "
                                    f"got cash {self.ds.get_last_cash()}")
                return self.hold(idx, dt_idx, price, trade_hold_code=ErrorCodes.ERROR_TH006)
            # [end if]

            # max_margin_loan check
            total_borrowed_qty = margin_used = self.ds.silo.get_total_pos()
            re_dict = self.cal_margin_level(price)
            account_equity_in_target_asset = re_dict.get("account_equity_in_target_asset")
            margin_level = re_dict.get("margin_level")

            safe_margin_loan_dict = self.get_safe_margin_loan(self.target_asset,
                                                              self.symbol,
                                                              total_borrowed_qty,
                                                              margin_level=margin_level,
                                                              index_price=price,
                                                              borrowed=margin_used)

            self.logger.debug(f"margin_level: {safe_margin_loan_dict.get('margin_level')}, qty: {safe_margin_loan_dict.get('qty')}")
            safe_margin_loan_qty = safe_margin_loan_dict.get("qty")
            margin_level = safe_margin_loan_dict.get("margin_level")

            # 檢查 margin_level
            if margin_level < d(self.margin_call_level):
                self.logger.warning(f"[BinanceTrade] ERROR_TH007 Short changes to HOLD due to "
                                    f"margin_level allowed: {margin_level} < {self.margin_call_level} ")
                return self.hold(idx, dt_idx, price, trade_hold_code=ErrorCodes.ERROR_TH007)
            # [end if]

            """
            這裡 safe_margin_loan_amt 是可以借出多少 target_asset 的 qty
            min_notional 是至少要買多少錢，是 home_asset, 不一樣
            safe_max_notional 是安全自最多可以花多少 home_asset 買
            """
            safe_max_notional = safe_margin_loan_qty * price
            if safe_max_notional <= self.min_notional * d(2):
                self.logger.warning("[BacktestOrder] ERROR_TH008 safe_max_notional <= self.min_notional * d(2) "
                                    "=> TradeAction.HOLD; Loanable amount too low")
                return self.hold(idx, dt_idx, price, trade_hold_code=ErrorCodes.ERROR_TH008)
            # [end if]

            # 重要的，必須符合 safe_margin_loan_qty
            safe_margin_loan_qty = d_round(safe_margin_loan_qty, self.ds.decimal_qty)
            short_num_positive = min(_short_num_positive, safe_margin_loan_qty)
            if short_num_positive != _short_num_positive:
                self.logger.warning(f">> short_num_positive is modified from {_short_num_positive} to "
                                    f"safe_margin_loan_qty {safe_margin_loan_qty}")
            self.logger.debug(f">> account_equity_in_target_asset: {account_equity_in_target_asset}, "
                             f"margin_level: {safe_margin_loan_dict.get('margin_level')}, "
                             f"qty: {safe_margin_loan_dict.get('qty')} {self.target_asset},"
                             f"total_amt_bought: {self.ds.silo.get_total_amt_bought()}, "
                             f"short_num:{short_num_positive} {self.target_asset}")

            if short_num_positive <= self.min_qty * d(2):
                self.logger.warning(f"[BinanceTrade] ERROR_TH002 Short failed due to "
                                    f"qty: {safe_margin_loan_dict.get('qty')} {self.target_asset}, "
                                    f"account_equity_in_target_asset: {account_equity_in_target_asset} {self.target_asset}")
                return self.hold(idx, dt_idx, price, trade_hold_code=ErrorCodes.ERROR_TH002)
            # [end if]

            # ======= START client execute ========
            orderId, executedQty, executedAmt, order = asyncio.run(
                self.exch_order.loan_then_short_selling_market(self.ds.symbol,
                                                               short_num_positive,
                                                               self.target_asset,
                                                               price=price))
            # 不小心發生事情，先繼續
            if orderId is None and executedQty == d(0):
                self.logger.warning("[BacktestOrder] ERROR_TH009 loan_then_short_selling_market failed => TradeAction.HOLD, "
                                    "program continues ....")
                return self.hold(idx, dt_idx, price, trade_hold_code=ErrorCodes.ERROR_TH009)
            # [end if]

            re = {"executedQty": executedQty, "executedAmt": executedAmt,
                  "orderId": orderId, "order": order}

            executedAmt = re["executedAmt"]
            executedQty = receivedQty_positive = re["executedQty"]
            receivedQty_negative = -receivedQty_positive

            # fee 是扣 quote_asset(home_asset)
            fee = d_round_fee(executedAmt * self.ds.short_cost_rate, self.ds.quote_comm_precision)

            orderId = re.get("orderId")

            # 更新價格
            price_executed_exact = executedAmt / executedQty
            price_executed = d_round(price_executed_exact, self.ds.decimal_price)
            self.ds.set_price(dt_idx, price_executed, self.app_env, True)

            # fee
            self.ds.set_fee1(fee)
            self.ds.set_fee2(d(0))

            # set_order_id
            self.ds.set_order_id(orderId, executedQty, executedAmt)
            # ======= END client execute ========

            # set_trade
            last_cash = self.ds.get_last_cash()
            last_borrowed_asset_qty = self.ds.get_last_borrowed_cash()
            cash = d_round(last_cash - fee + executedAmt, self.ds.quote_asset_precision)
            new_borrowed_cash_positive = last_borrowed_asset_qty + d_abs(executedQty)  # positive

            # trade_cash
            self.ds.set_trade_cash(self.ds.get_last_trade_cash())

            last_position = self.ds.get_last_position()
            # 兩個負的 last_position, receivedQty_negative
            new_position_negative = d_round(last_position + receivedQty_negative, self.ds.base_asset_precision)  # base_asset_precision
            new_asset = d_round(new_position_negative * price_executed, self.ds.quote_precision)
            self.ds.set_trade(TradeAction.SHORT, new_position_negative, cash, new_borrowed_cash_positive,
                              new_asset)

            # target_cash
            # TODO, 等待中時借的錢增加了 trade_target 但是還未反映出來
            target_cash_expect = target_cash_actual = self.ds.get_last_target_cash()
            if self.app_env == AppEnv.TRADE:
                i = 0
                while target_cash_expect != target_cash_actual and i < 10:
                    sleep(self.pause * 3)
                    target_cash_actual = self.get_target_balance()
                    i += 1
            self.logger.debug(f">> target_cash balance actual: {target_cash_actual}, trading min_qty: {self.min_qty}")

            if target_cash_expect != target_cash_actual:
                self.logger.warning(f"target_cash_expect actual {target_cash_actual} but got {target_cash_expect}")
            self.ds.set_target_cash(target_cash_actual)

            # update silos
            self.ds.silo.record_short(receivedQty_negative, d_negate(executedAmt), self.ds.is_significant_pos())

            # total_amt
            total_pos = d_round(self.ds.silo.get_total_pos(), self.ds.base_asset_precision)
            total_amt = self.ds.silo.get_total_amt_bought()

            paper_pnl = new_asset - total_amt
            paper_pnl_pct = d_round(paper_pnl / d_abs(total_amt), 5) if d_abs(total_amt) > 0 else d(0.)
            self.ds.set_total(paper_pnl, paper_pnl_pct, total_pos, total_amt)

            if not d_is_close(new_position_negative, total_pos, self.ds.base_asset_precision):
                raise Exception(f"silo positions {total_pos} differs from position {new_position_negative} when short")

            # drawdown
            self.cal_drawdown()

            # pnl
            cumulative_realized_pnl = self.ds.get_last_cumulative_realized_pnl() - fee
            realized_pnl = d_negate(fee)
            realized_pnl_pct = realized_pnl / executedAmt
            self.ds.set_pnl(realized_pnl, realized_pnl_pct, cumulative_realized_pnl)

            data = dict({
                "trade_action": TradeAction.SHORT,
                "price_executed": price_executed,
                "receivedQty": receivedQty_positive,
                "fee": fee,
                "fee_asset": self.target_asset,
                "realized_pnl": realized_pnl,
                "realized_pnl_pct": d_round(realized_pnl_pct, 5),
                "error_code": None
            }, **re)
            return data

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(err)
            sys.exit()

    def set_cash_change(self, idx: int, dt_idx: dt.datetime, price: Decimal,
                        new_cash: Decimal, new_target_cash: Decimal):
        """
        交易 cash 改變
        """
        try:
            if not isinstance(price, Decimal):
                raise Exception(f"set_cash_change()->price can only be type Decimal but got {type(price)}")

            # 更新價格
            self.ds.set_price(dt_idx, price, self.app_env, True)

            # fee
            self.ds.set_fee1(d(0))
            self.ds.set_fee2(d(0))

            self.ds.set_buysell_lvl(self.ds.get_last_buysell_lvl())

            # set_order_id
            self.ds.set_order_id(None, d(0), d(0))

            # set_trade
            last_position = self.ds.get_last_position()
            last_borrowed_cash = self.ds.get_last_borrowed_cash()
            asset = self.ds.silo.get_total_pos() * price
            self.ds.copy_last_trade(buysell=TradeAction.CASH_CHANGE, cash=new_cash, target_cash=new_target_cash)
            # trade_cash
            self.ds.set_trade_cash(self.ds.get_last_trade_cash())

            # target_cash
            target_cash = self.ds.get_last_target_cash()
            if self.app_env == AppEnv.TRADE:
                target_cash = self.get_target_balance()
            self.ds.set_target_cash(d(target_cash))

            # update silos
            # 沒有交易所以不用

            # total_amt
            total_pos = self.ds.silo.get_total_pos()
            total_amt = self.ds.silo.get_total_amt_bought()
            paper_pnl = asset - total_amt
            paper_pnl_pct = d_round(paper_pnl / d_abs(total_amt), 5) if d_abs(total_amt) > 0 else d(0.)
            self.ds.set_total(paper_pnl, paper_pnl_pct, total_pos, total_amt)

            # drawdown
            self.cal_drawdown()

            # pnl
            self._copy_last_cumulative_realized_pnl()

            self.logger.info(f"[BinanceTrade]\n"
                             f"===== DETECTED CASH CHANGE START =====\n"
                             f"  last_cash {self.ds.get_last_cash()} => new_cash {new_cash}\n"
                             f"  last_target_cash {self.ds.get_last_target_cash()} => new_cash {new_target_cash}\n"
                             f"===== DETECTED CASH CHANGE END =====\n")
            # return {
            #     "trade_action": TradeAction.CASH_CHANGE,
            #     "executedAmt": d(0),
            #     "executedQty": d(0),
            #     "fee": d(0),
            #     "fee_asset": self.target_asset,
            #     "realized_pnl": d(0),
            #     "realized_pnl_pct": d(0),
            #     "error_code": None
            # }
            return True

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(err)
            self.logger.error("[BacktestOrder] def cash_change app encountered error, but continue to run next step")
            return False

    def set_trade_cash_change(self, idx: int, dt_idx: dt.datetime, price: Decimal, new_trade_cash: Decimal):
        """
        交易錢 trade_cash 的限制改變
        """
        try:
            if not isinstance(price, Decimal):
                raise Exception(f"set_trade_cash_change()->price can only be type Decimal but got {type(price)}")

            # 更新價格
            self.ds.set_price(dt_idx, price, self.app_env, True)

            # fee
            self.ds.set_fee1(d(0))
            self.ds.set_fee2(d(0))

            self.ds.set_buysell_lvl(self.ds.get_last_buysell_lvl())

            # set_order_id
            self.ds.set_order_id(None, d(0), d(0))

            # set_trade
            asset = self.ds.silo.get_total_pos() * price
            self.ds.copy_last_trade(buysell=TradeAction.TRADE_CASH_CHANGE)
            self.ds.set_trade_cash(new_trade_cash)

            # target_cash
            target_cash = self.ds.get_last_target_cash()
            if self.app_env == AppEnv.TRADE:
                target_cash = self.get_target_balance()
            self.ds.set_target_cash(d(target_cash))

            # update silos
            # 沒有交易所以不用

            # total_amt
            total_pos = self.ds.silo.get_total_pos()
            total_amt = self.ds.silo.get_total_amt_bought()
            paper_pnl = asset - total_amt
            paper_pnl_pct = d_round(paper_pnl / d_abs(total_amt), 5) if d_abs(total_amt) > 0 else d(0.)
            self.ds.set_total(paper_pnl, paper_pnl_pct, total_pos, total_amt)

            # drawdown
            self.cal_drawdown()

            # pnl
            self._copy_last_cumulative_realized_pnl()

            # if successful, set it to zero
            set_reset_trade_cash(d(0), self.user_bot_id, self.logger)

            self.logger.info(f"[BinanceTrade]\n"
                             f"===== RESET TRADE CASH START =====\n"
                             f"  last_trade_cash {self.ds.get_last_trade_cash()} => new_trade_cash {new_trade_cash}\n"
                             f"  set RESET_TRADE_CASH.txt to zero: {get_reset_trade_cash_txt(self.logger)}\n"
                             f"===== RESET TRADE CASH END =====\n")
            # return {
            #     "trade_action": TradeAction.CASH_CHANGE,
            #     "executedAmt": d(0),
            #     "executedQty": d(0),
            #     "fee": d(0),
            #     "fee_asset": self.target_asset,
            #     "realized_pnl": d(0),
            #     "realized_pnl_pct": d(0),
            #     "error_code": None
            # }
            return True

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(err)
            self.logger.error("[BacktestOrder] def cash_change app encountered error, but continue to run next step")
            return False

    def get_home_balance(self, **kwargs):
        """
        取得 spot market 的 home asset(quote asset)
        IMPORTANT:後面有接 BinanceOrder/DummyOrder
        請注意：**kwargs是用來跟 DummyOrder 配合來注入取得 c.ds.cash/c.ds.target_cash
        """
        if kwargs.get("margin_level") is None and self.app_env == AppEnv.TRAIN:
            kwargs["margin_level"] = d(999)

        acct = self.exch_order.get_account_balance(self.home_asset, self.spot_margin, self.symbol,
                                                   **kwargs)
        balance = d(acct[self.home_asset]["free"])
        return balance

    def get_target_balance(self, **kwargs):
        """
        取得 spot market 的 target_asset(base_asset)
        IMPORTANT:後面有接 BinanceOrder/DummyOrder
        請注意：**kwargs是用來跟 DummyOrder 配合來注入取得 c.ds.cash/c.ds.target_cash
        """
        if kwargs.get("margin_level") is None and self.app_env == AppEnv.TRAIN:
            kwargs["margin_level"] = d(999)

        acct = self.exch_order.get_account_balance(self.target_asset,
                                                   self.spot_margin,
                                                   self.symbol,
                                                   **kwargs)
        balance = d(acct[self.target_asset]["free"])
        return balance

    def check_fee_required(self, **kwargs) -> (d, d):
        """
        檢查現有 ** margin account** 裡面的 target_asset 餘額是否足夠付手續費
        """
        try:
            if kwargs.get("margin_level") is None and self.app_env == AppEnv.TRAIN:
                kwargs["margin_level"] = d(999)

            if kwargs.get("balance") is None and self.app_env == AppEnv.TRAIN:
                kwargs["balance"] = self.ds.get_last_target_cash()

            active_margin_balance = self.get_target_balance(margin_level=kwargs.get("margin_level"),
                                                            balance=kwargs.get("balance"))
            fee_required = d_abs(self.ds.get_last_position()) * self.ds.cover_cost_rate * self.ds.cover_cost_rate

            return active_margin_balance, fee_required

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"BacktestOrder.check_fee_required {err}")
            sys.exit()

    def do_check_fee(self, price, **kwargs):
        try:
            active_balance, fee_required = self.check_fee_required(**kwargs)
            self.logger.debug("[BacktestOrder] do_check_fee starting")

            is_zero = d_is_close(fee_required, d(0.0), self.ds.base_asset_precision)
            if not is_zero and active_balance < fee_required * d(10):
                self.logger.debug(f"[BacktestOrder] BacktestOrder.do_check_fee: Insufficient Fee, "
                                  f"active:{active_balance:.8f} < required:{fee_required:.8f} * 10")

                dt_idx = self.ds.get_dt_idx()

                self._buy_fee(dt_idx, dt_idx, fee_required * d(10), price)
                self.logger.debug("[BacktestOrder] do_check_fee buy_fee done")

                return True

            self.logger.debug("[BacktestOrder] do_check_fee returns False")
            return False

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(f"BacktestOrder.do_check_fee {err}")
            return False

    def _buy_fee(self, idx: int, dt_idx: dt.datetime, buy_num: Decimal, price: Decimal):
        try:
            if not isinstance(price, Decimal):
                raise Exception(f"_buy_fee()->price can only be type Decimal but got {type(price)}")

            self.logger.debug(f"[BacktestOrder] _buy_fee, buy_num:{buy_num}")

            # 更新價格
            self.ds.set_price(dt_idx, price, self.app_env, True)

            # 計算交易 qty
            buy_num = d_round(buy_num, self.ds.decimal_qty)

            # min qty check
            buy_num = self.min_qty * d(2) if buy_num < self.min_qty else buy_num

            # cannot be less than self.min_notional.
            amount = price * buy_num
            while amount < self.min_notional:
                buy_num = self.min_notional * d(1.1) / price
                buy_num = d_round(buy_num, self.ds.decimal_qty)
                amount = buy_num * price
                self.logger.debug(f"[BacktestOrder] def buy_fee 2:\nbuy_num={buy_num}, amount={amount}")

                if buy_num == d(0.0):
                    buy_num = d(1 * 10 ** (-self.ds.decimal_qty))
                    buy_num = d_round(buy_num * d(1.1), self.ds.decimal_qty)
                    amount = buy_num * price
                    self.logger.debug(f"[BacktestOrder] def buy_fee 3:\nbuy_num={buy_num}, amount={amount}")

            # not enough money, hold
            if buy_num * price > self.ds.get_last_cash() * d(0.98):
                self.logger.warn(f"[BinanceTrade] Buy failed due to short of cash got {self.ds.get_last_cash()}")
                return

            # ======= START client execute ========
            # TODO: 改成在 margin account 買
            orderId, executedQty, executedAmt, order = asyncio.run(
                self.exch_order.buy_market(self.ds.symbol, buy_num, price=price))
            re = {"executedQty": executedQty, "executedAmt": executedAmt,
                  "orderId": orderId, "order": order}

            executedAmt = re.get("executedAmt")
            executedQty = receivedQty = re.get("executedQty")
            fee = d(0.0)
            if self.ds.exch_mode == "SpotAPI":
                fee = d_round_fee(buy_num * self.ds.buy_cost_rate, self.ds.quote_comm_precision)
                receivedQty = d_round(buy_num - fee, self.ds.quote_comm_precision)

            orderId = re["orderId"]

            # 更新價格
            price_executed_exact = executedAmt / executedQty
            price_executed = d_round(price_executed_exact, self.ds.decimal_price)
            self.ds.set_price(dt_idx, price_executed, self.app_env, True)

            # fee
            self.ds.set_fee1(fee)
            self.ds.set_fee2(d(0))

            # set_order_id
            self.ds.set_order_id(orderId, executedQty, executedAmt)
            # ======= END client execute ========

            # set_trade
            last_cash = self.ds.get_last_cash()
            last_borrowed_cash = self.ds.get_last_borrowed_cash()
            cash = last_cash - executedAmt - fee  # 扣掉，因為已支出
            new_position = self.ds.get_last_position()
            new_asset = self.ds.get_last_asset()
            # new_asset = self.ds.get_asset(idx - 1) + executedAmt
            self.ds.set_trade(TradeAction.BUY_FEE, new_position, cash, last_borrowed_cash, new_asset)

            # target_cash
            target_cash_new = self.ds.get_last_target_cash() + executedQty
            self.ds.set_target_cash(target_cash_new)

            # update silos
            # 買 target_cash 手續費所以不用

            # total_amt
            total_pos = self.ds.silo.get_total_pos()
            total_amt = self.ds.silo.get_total_amt_bought()
            paper_pnl = new_asset - total_amt
            paper_pnl_pct = d_round(paper_pnl / d_abs(total_amt), 5) if d_abs(total_amt) > 0 else d(0.)
            self.ds.set_total(paper_pnl, paper_pnl_pct, total_pos, total_amt)

            # drawdown
            self.cal_drawdown()

            # pnl
            self._copy_last_cumulative_realized_pnl()

            data = dict({
                "trade_action": TradeAction.BUY_FEE,
                "price_executed": price_executed,
                "receivedQty": receivedQty,
                "fee": fee,
                "fee_asset": self.target_asset
            }, **re)

            return data

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(err)
            sys.exit()

    def check_adjust_pos(self, dt_idx, price, txn_order_filename):
        """
        把要改的資料輸入後自動調整其他欄位
        可能是因為存入新的部位，或是程式連線問題而導致的資料錯誤或資料丟失
        把 position+total_amt_bought 為一組，輸入後會去調整
        另一組是 cumulative_realized_pnl_diff,
        他是用來調整從另一個檔案轉換過來需要把之前的 cumulative_realized_pnl 帶過來然後做加減（不是取代）

        你可以一起用，但是同一組的必須資料都填
        不用的那組請放 None
        """
        try:
            adjust_txn_order_dir = f"{project_root}/appData/user_tradebot/{self.user_bot_id}/tradeBot/ADJUST_TXN_ORDER.csv"

            if not path.exists(adjust_txn_order_dir):
                return

            # REFERENCE: 教你如何判別 NA(float type) 和 None(None class type)
            # https://stackoverflow.com/a/75984091/1596886
            # https://datascience.stackexchange.com/questions/117423/pands-doesnt-recognize-missing-values-in-csv
            adjust_txn_order = pd.read_csv(adjust_txn_order_dir,
                                           usecols=["row", "position_diff", "total_amt_bought_diff",
                                                    "cumulative_realized_pnl_diff"],
                                           index_col=0,
                                           keep_default_na=True,  # <----- 注意
                                           na_values="None",  # <----- 注意
                                           parse_dates=False,
                                           dtype={
                                               "row": int,
                                               "position_diff": float,
                                               "total_amt_bought_diff": float,
                                               "cumulative_realized_pnl": float
                                           }
                                           # , converters={
                                           #    "position": lambda v: d(v) if not pd.isna(v) else np.nan,
                                           #    "total_amt_bought": lambda v: d(v) if not pd.isna(v) else np.nan,
                                           #    "cumulative_realized_pnl": lambda v: d(v) if not pd.isna(v) else np.nan
                                           # }
                                           )

            if adjust_txn_order.isnull().all().all():
                return

            if adjust_txn_order is not None and adjust_txn_order.shape[0] >= 1:
                idx = self.ds.get_idx()
                self.logger.debug(f"[BinanceTrade] ERROR_TH010 idx: {idx}, adjust_txn_order: {adjust_txn_order}")

                # 先 copy 一份過來，然後再改資料
                # 要注意 position, buysell, total_position, silo, asset, asset_cash
                self.hold(idx, dt_idx, price, trade_hold_code=ErrorCodes.ERROR_TH010)
                self.ds.copy_last_trade(TradeAction.ADJUST)

                for index, row in adjust_txn_order.iterrows():
                    # save to csv
                    data_dict = self.ds.get_current_row()
                    data_dict["idx"] = idx
                    # order_txn_dict = self.ds.get_just_executed()
                    # data_dict.update(order_txn_dict)

                    # 修改 cumulative_realized_pnl
                    # 注意： cumulative_realized_pnl 和 cumulative_realized_pnl_diff 不同

                    if not pd.isna(row["cumulative_realized_pnl_diff"]):
                        last_cumulative_realized_pnl = self.ds.get_last_cumulative_realized_pnl()
                        data_dict[
                            "cumulative_realized_pnl"] = cumulative_realized_pnl = last_cumulative_realized_pnl + d(
                            row["cumulative_realized_pnl_diff"])
                        self.ds.set_cumulative_realized_pnl(cumulative_realized_pnl)
                        self.logger.info(f"[BinanceTrade] def check_adjust_pos.cumulative_realized_pnl is set from "
                                         f"{last_cumulative_realized_pnl} to {cumulative_realized_pnl}")

                    # 修改 position
                    if not pd.isna(row["position_diff"]) and not pd.isna(row["total_amt_bought_diff"]):
                        position = self.ds.get_last_position() + d(row["position_diff"])
                        total_amt_bought = self.ds.get_last_total_amt_bought() + d(row["total_amt_bought_diff"])

                        if self.ds.get_target_cash() < position:
                            raise Exception(
                                f"[BinanceTrade] def check_adjust_pos.target_cash: {self.ds.get_target_cash()} is smaller than "
                                f"position requested: {position}"
                                ", please check ....")

                        data_dict["position"] = data_dict["total_position"] = position
                        data_dict["total_amt_bought"] = total_amt_bought
                        data_dict["silo_pos"] = position
                        data_dict["silo_amt"] = total_amt_bought
                        new_asset = d_round(position * price, self.ds.quote_precision)
                        paper_pnl = new_asset - total_amt_bought
                        data_dict["paper_pnl"] = paper_pnl
                        paper_pnl_pct = d_round(paper_pnl / d_abs(total_amt_bought), 5) if d_abs(
                            total_amt_bought) > 0 else d(0.)

                        self.ds.set_position(position)
                        self.ds.set_total(paper_pnl, paper_pnl_pct, position, total_amt_bought)

                        self.ds.silo.reset()
                        self.ds.silo.record_buy(position, total_amt_bought, False)  # overwrite everything
                        self.logger.info(f"[BinanceTrade] def check_adjust_pos\n"
                                         f"position is set from "
                                         f"{self.ds.get_last_position()} to {position}\n"
                                         f"{self.ds.get_last_total_position()} to {position}\n"
                                         f"{self.ds.get_last_total_amt_bought()} to {total_amt_bought}\n"
                                         f"{self.ds.get_last_asset()} to {new_asset}\n"
                                         f"{self.ds.get_last_paper_pnl()} to {paper_pnl}\n"
                                         f"{self.ds.get_last_paper_pnl_pct()} to {paper_pnl_pct}\n"
                                         )

                    order_txn_dict = self.ds.get_just_executed()
                    data_dict.update(order_txn_dict)

                    # 檢查欄位是否正確
                    cols = ["dt_idx"] + list(data_dict.keys())  # because it is index name
                    if set(cols) != set(self.ds.trade_cols):
                        raise Exception(f"[BinanceTrade] def check_adjust_pos.txn_order col expect {cols} \n"
                                        f"got {self.ds.trade_cols} \n"
                                        f"differ {return_not_matches(cols, self.ds.trade_cols)}")


                    # 確認欄位位置排列正確
                    trade_data_new = pd.DataFrame.from_records([data_dict], columns=self.ds.trade_cols)
                    trade_data_new.set_index("dt_idx", inplace=True)

                    # append result
                    trade_data_new.to_csv(txn_order_filename, mode="a", header=False, index=True,
                                          na_rep="None")

                    # IMPORTANT：確保 caller 裡計算好的 tech_ary copy 過來
                    self.ds.copy_last_tech_ary()
                    self.ds.set_price(dt_idx, price, self.app_env)

                    self.ds.step_idx(self.app_env)  # idx += 1
                    self.ds.copy_last_paper_pnl()

            # [for loop]

            # reset to NA
            for i, row in adjust_txn_order.iterrows():
                adjust_txn_order.at[i, "position_diff"] = np.nan
                adjust_txn_order.at[i, "total_amt_bought_diff"] = np.nan
                adjust_txn_order.at[i, "cumulative_realized_pnl_diff"] = np.nan

            adjust_txn_order.to_csv(adjust_txn_order_dir, mode="w", header=True, index=True,
                                    na_rep="NA")

            print("testing finished, existing")
            sys.exit()

        except Exception as e:
            _err_str = readable_error(e, __file__)
            self.logger.error(f"[BinanceTrade] {_err_str}")
            sys.exit()

    def save_trade(self, data, data_path: str, exchange: str, symbol: str, interval: str,
                   append: bool = False, logger=None):
        try:
            # folder location
            dest_dir = "{0}/exchange={1}/symbol={2}/interval={3}".format(data_path, exchange, symbol, interval)

            train_data_schema = {
                "price": "float64",
                "buy_tracer": "float64",
                "buy_alfa": "float64",
                "buy_sd_mv": "float64",
                "sell_tracer": "float64",
                "sell_alfa": "float64",
                "sell_sd_mv": "float64"
            }
            save_data(data, dest_dir, train_data_schema, append)
        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.error(err)
            raise Exception(e)

    def _copy_last_cumulative_realized_pnl(self):
        self.ds.set_pnl(d(0), d(0), self.ds.get_last_cumulative_realized_pnl())

    def cal_drawdown(self):
        """
        這裡 drawdown 是 absolute value, 百分比是 drawdown_pct
        但是大家算法可能應情況不同，所以如果要就 overwrite 這個 function 也可
        Parameters
        ----------
        idx (int): idx
        drawdown (decimal): 指定數字，沒有的畫就是用 _paper_pnl
        """
        idx = self.ds.get_idx()
        # paper_pnl, paper_pnl_pct = self.ds.cal_paper_pnl_run(price)
        paper_pnl = self.ds.get_paper_pnl()
        paper_pnl_pct = self.ds.get_paper_pnl_pct()

        # absolute 數字
        last_drawdown = self.ds.get_last_drawdown()

        # 百分比
        last_drawdown_pct = self.ds.get_last_drawdown_pct()
        if paper_pnl_pct < last_drawdown_pct:
            self.ds.set_drawdown(paper_pnl)
            self.ds.set_drawdown_pct(paper_pnl_pct)
        else:
            self.ds.set_drawdown(last_drawdown)
            self.ds.set_drawdown_pct(last_drawdown_pct)

    def cal_margin_level(self, price):
        try:
            margin_used = self.ds.silo.get_total_pos()
            target_cash = self.ds.get_last_target_cash()
            account_equity_in_target_asset = d_round(
                (self.ds.get_last_cash() / price + target_cash - d_abs(margin_used)) * self.user_leverage_ratio,
                self.ds.base_asset_precision)  # total_amt_bought 是負的
            margin_level = d_round(account_equity_in_target_asset / margin_used + d(1), 2) if margin_used > 0 else d(999)

            return {
                "margin_level": margin_level,
                "account_equity_in_target_asset": account_equity_in_target_asset
            }


        except Exception as e:
            self.logger.error(readable_error(e, __file__))
            sys.exit()

    def get_safe_margin_loan(self, asset: str, isolatedSymbol: str,
                             margin_used: Decimal, **kwargs):
        """
        Account Equity  = Total Balance - Debt
        假設 ETHUSDT = $10
        TOTAL BALANCE: 200 USDT / $10 + 1ETH(原本擁有) = 21ETH
        Debt = 2ETH
        Equity - 21ETH -2ETH = 19ETH

        計算出可以借出來多少和現有 margin_level 是多少, 幣安文件沒寫清楚，還需要加上 +1
        margin_level = account_equity/margin_used + 1

        margin_used = initial_borrowed_qty

        Parameters
        ----------
        :param asset:
        :param margin_used:
        :param isolatedSymbol:

        :param kwargs:
            - :param index_price: required for AppEnv.TRAIN
                used for get_margin_info
            - :type Decimal
            - :param margin_level: required for AppEnv.TRAIN
                used for get_margin_info
            - :type Decimal
            - :param borrowed: required for AppEnv.TRAIN
                used for get_margin_info
            - :type Decimal
        """
        try:
            acct = self.exch_order.get_margin_info(asset, "isolated", isolatedSymbol,
                                                   margin_level=kwargs.get("margin_level"),
                                                   index_price=kwargs.get("index_price"),
                                                   borrowed=margin_used)

            index_price = d(acct.get("indexPrice"))
            borrowed = acct.get(asset, None).get("borrowed", d(0)) or d(0)
            sys_margin_level = d(acct.get("marginLevel"))
            if d_abs(borrowed) != d_abs(margin_used):
                self.logger.warning(f"borrowed: {borrowed}, margin_used: {margin_used} are different.")

            # this equity is in target_asset aspect
            last_cash = self.ds.get_last_cash()
            target_cash = self.ds.get_last_target_cash()
            # 我們不加入 target_asset 因為我們以 home_asset 為主要 collateral
            account_equity_in_target_asset = d_round((last_cash / index_price + target_cash - d_abs(margin_used)) * self.user_leverage_ratio, self.ds.base_asset_precision)
            a = (account_equity_in_target_asset / d_abs(margin_used) + d(1)) if margin_used != 0 else 999
            margin_level = d_round(a, 6) if margin_used != 0 else d(a)
            margin_level = min(margin_level, d(999))
            # 之前 乘以 0.8 是因為 liquidity ratio 大約是 1.2 左右，這裡可以再進步 (1/1.2 =~ 0.83)
            # 但我們不再這麼做，給出真實的，希望怎麼處理另外在說
            safe_margin_loan_qty = d_round(account_equity_in_target_asset - d_abs(margin_used), self.ds.base_asset_precision)
            safe_margin_loan_qty = max(safe_margin_loan_qty, d(0))

            return {
                "qty": safe_margin_loan_qty,
                "margin_level": min(margin_level, sys_margin_level)
            }
        except Exception as e:
            self.logger.error(readable_error(e, __file__))
            sys.exit()
