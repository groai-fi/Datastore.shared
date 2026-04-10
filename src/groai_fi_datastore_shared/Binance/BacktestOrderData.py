import sys
import time
import numpy as np
import datetime as dt
from decimal import Decimal
from typing import Deque, Dict, Any
from collections import deque
from typing import Optional

from .enums import AppEnv
from .BinanceOrderData import BinanceOrderData
from .utils import d, d_round, d_is_close, d_abs, d_negate
from .utils import convert_to_min, readable_error


class Kelly:
    # init_p = 0.505
    # init_b_win = 0.001
    # init_b_loss = 0.001
    def __init__(self, p=0.51, b_win=2, b_loss=2):
        self.p = p
        self.b_win = b_win  # assume initial win is 2% gain
        self.b_loss = b_loss  # assume initial lose is 2% loss
        self.b = self.b_win / self.b_loss

        self.kelly_cap = self.p - (1 - self.p) / self.b

    def update_p(self, p):
        self.p = float(p)

    def update_b(self, win, loss):
        self.b_loss, self.b_win = max(abs(loss), 0.01), max(abs(win), 0.01)
        self.b = float(self.b_win) / float(self.b_loss)

    def cal_kelly_cap(self):
        self.b = float(self.b_win) / float(self.b_loss)

        self.kelly_cap = round(self.p - (1 - self.p) / self.b, 5)
        """
        # FOR DEBUGGING
        print(f"kelly_cap:{self.kelly_cap:.2f} = "
              f"p:{self.p:.5f} - ( [1-p]:{1-self.p:.5f}) / b:{self.b:.5f}"
              f"b = win:{self.b_win:.5f} / "
              f"loss:{self.b_loss:.5f}")
        """
        return self.kelly_cap


class Silo:

    def __init__(self, quote_asset_precision, logger, size=5):
        self._size = size
        self.logger = logger
        self.position: Deque[Optional[Decimal]] = deque(maxlen=10)
        self.amt: Deque[Optional[Decimal]]  = deque(maxlen=10)
        if quote_asset_precision is None:
            raise Exception("quote_asset_precision cannot be None")
        self.quote_asset_precision = quote_asset_precision

    def reset(self):
        self.position = deque(maxlen=10)
        self.amt = deque(maxlen=10)

    def get_size(self):
        """
        max capacity this Silo is allowed to hold
        """
        return self._size

    def can_buy(self, is_significant_pos: int) -> bool:
        if is_significant_pos == 1:
            _can_buy = len(self.position) < self._size
        elif is_significant_pos == -1:
            _can_buy = False
        else:
            _can_buy = True
        return _can_buy

    def can_sell(self, is_significant_pos: int) -> bool:
        # position is
        return len(self.position) > 0 and is_significant_pos == 1

    def can_short(self, is_significant_pos: int) -> bool:
        if is_significant_pos == -1:
            _can_short = len(self.position) < self._size
        elif is_significant_pos == 1:
            _can_short = False
        else:
            _can_short = True
        return _can_short

    def can_cover(self, is_significant_pos: int) -> bool:
        return len(self.position) > 0 and is_significant_pos == -1

    def record_buy(self, position: Decimal, amt: Decimal, is_significant_pos: int):
        if self.can_buy(is_significant_pos):
            self.position.append(position)
            self.amt.append(amt)

    def record_sell(self,
                    position_req: Decimal,
                    amt_req: Decimal,
                    position_expected: Decimal,
                    decimal_qty: int):
        """
        特別注意：
        下面不要用(1-pct) 然後 round 這樣的方式，會不精確。要先計算資料then round, 然後去跟其他做加減。
        """
        threshold = d(f'1e-{decimal_qty+1}')
        initial_bought_amt = d(0.0)
        _position_req_tmp = position_req

        # TODO 這裡有問題， decimal_qty 是 5 但是 app 卻是 4, 最後結果是 SpotTest 和S potAPI 規格可能不同 => 正常
        while position_req > 0:  # and not d_is_close(position_req, threshold, decimal_qty):
            # if required more than the current slot
            if position_req >= self.position[0]:
                # deal with position
                # print(f"position_req {position_req} -= self.position[0] {self.position[0]}")
                position_req -= self.position[0]
                initial_bought_amt += self.amt[0]
                self.position.popleft()
                self.amt.popleft()
            else:
                # 特別注意的地方
                tmp_initial_bought_amt = d_round(self.amt[0] * (position_req / self.position[0]),
                                                 self.quote_asset_precision)
                self.logger.debug(f"record_sell: {tmp_initial_bought_amt}, self.amt[0]:{self.amt[0]}")
                initial_bought_amt += tmp_initial_bought_amt

                # deal with position/ amt
                self.amt[0] -= tmp_initial_bought_amt
                # print(f"self.position[0] {self.position[0]} -= position_req {position_req}")

                self.position[0] -= position_req
                position_req = d(0.0)
                # no need to move cursor because something remains

            if len(self.position) == 0:
                break

        realized_pnl = amt_req - initial_bought_amt

        silo_pos = self.get_total_pos()
        # print(f"position_expected:{position_expected} self.position: {self.position} np.sum(self.position): {np.sum(self.position)}")
        if not d_is_close(position_expected, silo_pos, self.quote_asset_precision):
            self.logger.error(f"sell_num: {_position_req_tmp}, "
                              f"silo position: {self.position}, "
                              f"position_expect: {position_expected}, "
                              f"threshold: {threshold}, "
                              f"decimal_qty: {decimal_qty} ")
            raise Exception(f"record_sell() silo positions {silo_pos} differs from "
                            f"position expected {position_expected}")

        return d_round(realized_pnl, self.quote_asset_precision), d_round(realized_pnl / initial_bought_amt, 5)

    def record_short(self, position: Decimal, amt: Decimal, is_significant_pos: int):
        if position >= d(0.0):
            raise Exception("short position cannot be positive")

        if amt >= d(0.0):
            raise Exception("short amount cannot be positive")

        if self.can_short(is_significant_pos):
            self.position.append(position)
            self.amt.append(amt)

    def record_cover(self, position_req_positive: Decimal,
                     amt_executed_positive: Decimal,
                     decimal_qty: int,
                     position_expected: Decimal):
        threshold = d(f"1e-{decimal_qty+1}")
        initial_short_amt = d(0.0)
        remain_qty = d_negate(position_req_positive)

        while remain_qty < 0:  # not d_is_close(remain_qty, threshold, decimal_qty):
            # if required more than the current slot
            if d_abs(remain_qty) >= d_abs(self.position[0]):
                remain_qty -= self.position[0]  # position 是負的
                initial_short_amt += self.amt[0]  # amt 是負的
                self.position.popleft()
                self.amt.popleft()
            else:
                # amt, only portion
                tmp_initial_bought_amt = d_round(self.amt[0] * d_abs(remain_qty / self.position[0]),
                                                 self.quote_asset_precision)  # negative for cover
                initial_short_amt += tmp_initial_bought_amt

                tmp_amt = self.amt[0] - tmp_initial_bought_amt
                self.amt[0] = tmp_amt
                self.position[0] -= remain_qty
                remain_qty = d(0.0)

            if len(self.position) == 0:
                break

        realized_pnl = d_abs(initial_short_amt) - amt_executed_positive  # buy back cheaper than we earn

        silo_pos = self.get_total_pos()
        if not d_is_close(position_expected, silo_pos, self.quote_asset_precision):
            raise Exception(f"record_cover() silo positions {silo_pos} differs from "
                            f"position expected {position_expected} remain_qty={remain_qty}")

        return d_round(realized_pnl, self.quote_asset_precision), d_round(realized_pnl / abs(initial_short_amt), 5)

    def populate_pos_amt(self, pos: deque, amt: deque):
        if not isinstance(pos, deque):
            raise Exception(f"silo position population failed, expect deque but got {type(pos)} ")

        if not isinstance(amt, deque):
            raise Exception(f"silo amt population failed, expect deque but got {type(amt)} ")

        if len(pos) > self._size + 1:
            raise Exception(f"silo position population expect len {self._size} or {self._size + 1} but got {len(pos)}")

        if len(amt) > self._size + 1:
            raise Exception(f"silo amount population expect len {self._size} or {self._size + 1}  but got {len(amt)}")

        self.position = pos
        self.amt = amt

    def get_total_pos(self):
        return d(np.sum(self.position))

    def get_total_amt_bought(self):
        return d(np.sum(self.amt))

    def get_position_str(self):
        return "|".join([str(item) for item in self.position])

    def get_amt_str(self):
        return "|".join([str(item) for item in self.amt])


class BacktestOrderData(BinanceOrderData):
    def __init__(self,
                 symbol: str,
                 home_asset: str,
                 target_asset: str,
                 exch_api: any,
                 silo_size: int,
                 init_trade_cash: Decimal,
                 init_target_cash: Decimal,
                 exch_mode: str,
                 kelly_cap_args: Dict[str, Any],
                 logger: any
                 ):

        if symbol is None:
            raise Exception("BacktestOrderData => missing symbol")

        super().__init__(symbol,
                         home_asset,
                         target_asset,
                         logger)

        if exch_mode is None:
            raise Exception("exch_mode cannot be None")

        self.exch_api = exch_api
        self.kelly_cap_args = kelly_cap_args

        # TODO，換成 view 和 trade 類別
        # 設定幾位數
        self.decimal_qty = None  # for trading
        self.decimal_price = None  # for trading

        self.decimal_qty_view = None  # displaying purpose only, not for trading
        self.decimal_price_view = None  # displaying purpose only, not for trading

        self.init_decimal_price_qty()

        self.base_asset, self.base_asset_precision, self.quote_asset, self.quote_precision, self.quote_asset_precision, self.base_comm_precision, self.quote_comm_precision = None, None, None, None, None, None, None
        self.init_precision()

        # 用來儲存連續交易的紀錄
        # keeping track of cumulative_realized_pnl buy/sell position and cost
        self.silo = Silo(self.quote_asset_precision, self.logger, silo_size)

        self.buy_cost_rate = d(0.001)
        self.sell_cost_rate = d(0.001)
        self.short_cost_rate = d(0.001)
        self.cover_cost_rate = d(0.001)

        if not isinstance(init_trade_cash, Decimal):
            raise Exception("BacktestOrderData => init_trade_cash must be of type Decimal")
        if not isinstance(init_target_cash, Decimal):
            raise Exception("BacktestOrderData => init_target_cash must be of type Decimal")

        self.init_trade_cash: Decimal = init_trade_cash
        self.init_target_cash: Decimal = init_target_cash

        self.exch_mode = exch_mode

        # 記錄交易次數
        self.num_trade = 0

        # target_asset_reserve
        self.min_qty = self.exch_api.get_spec(symbol).get("min_qty")
        self.min_notional = self.exch_api.get_spec(symbol).get("min_notional")

        # init all data
        self.is_initialized = False

        self.KellyCls = None
        self._kelly_p: Optional[Deque[Optional[float]]] = None
        self._kelly_b_win: Optional[Deque[Optional[float]]] = None
        self._kelly_b_loss: Optional[Deque[Optional[float]]] = None
        self._kelly_cap: Optional[Deque[Optional[float]]] = None
        self._buysell_lvl: Optional[Deque[Optional[float]]] = None

        if self.kelly_cap_args.get("min_kelly_cap", None) is None:
            self.kelly_cap_args["min_kelly_cap"] = 0.01

        if self.kelly_cap_args.get("min_kelly_cap") < 0.01:
            self.kelly_cap_args["min_kelly_cap"] = 0.01
            self.logger.warning("min_kelly_cap has to be at least 0.01 ( 1% )")


    def reset(self):
        super().reset()
        self.silo.reset()

        # Cash
        assert len(self._cash) == 1
        self._cash[0] = self.init_trade_cash  # only cash initially
        self._cash_asset[0] = self.init_trade_cash  # only cash initially
        self._target_cash[0] = self.init_target_cash  # only cash initially
        self._trade_cash[0] = self.init_trade_cash  # only cash initially

        # 記錄交易次數
        self.num_trade = 0
        # 重要
        self.is_initialized = True

        # kelly cap  # REQUIRED IN STEP
        self._kelly_cap = deque([None], maxlen=self.max_deque)
        self.KellyCls = None
        self.reset_kelly_cal()

    def init_decimal_price_qty(self):
        if self.decimal_qty_view is None:
            re = self.exch_api.get_decimal_price_qty(self.symbol)
            self.decimal_qty_view = re.get("decimal_qty_view", None)
            self.decimal_price_view = re.get("decimal_price_view", None)
            self.decimal_qty = re.get("decimal_qty", None)
            self.decimal_price = re.get("decimal_price", None)

    def init_precision(self):
        if self.quote_precision is None:
            p = self.exch_api.get_precision(self.symbol)
            self.base_asset = p.get("base_asset", None)
            self.base_asset_precision = p.get("base_asset_precision", None)
            self.quote_asset = p.get("quote_asset", None)
            self.quote_precision = p.get("quote_precision", None)
            self.quote_asset_precision = p.get("quote_asset_precision", None)
            self.base_comm_precision = p.get("base_comm_precision", None)
            self.quote_comm_precision = p.get("quote_comm_precision", None)

    def step_idx(self, app_env: AppEnv):
        if self.decimal_qty is None or self.decimal_price is None or \
                self.decimal_qty_view is None or self.decimal_price_view is None:
            raise Exception("decimal_qty and decimal_price have to be set before using")

        assert self.is_initialized

        self._idx += 1

        self._dt_idx.append(None)
        self.price_ary.append(None)

        self._position.append(None)
        self._buysell.append(None)
        self._asset.append(None)
        self._cash.append(None)
        self._borrowed_cash.append(None)
        self._target_cash.append(None)
        self._cash_asset.append(None)

        self._realized_pnl.append(None)
        self._realized_pnl_pct.append(None)

        self._fee1.append(None)
        self._fee2.append(None)
        self._executed_qty.append(None)
        self._executed_amt.append(None)
        self._paper_pnl.append(None)
        self._paper_pnl_pct.append(None)
        self._total_position.append(None)
        self._total_amt_bought.append(None)

        # self._silo_pos.append(None)
        # self._silo_amt.append(None)

        self._trade_cash.append(None)
        self._orderId.append("")
        self._drawdown.append(None)
        self._drawdown_pct.append(None)

        self._cumulative_realized_pnl.append(None)

        self._kelly_p.append(0)
        self._kelly_b_win.append(0)
        self._kelly_b_loss.append(0)
        self._kelly_cap.append(0)

        # 檢查長度一樣
        deque_len = len(self._dt_idx)
        assert len(self.price_ary) == deque_len, \
            f"len(price_ary) must match len(_dt_idx) got len {len(self.price_ary)}"

        deque_len = len(self._position)
        assert len(self._buysell) == deque_len and \
            len(self._buysell) == deque_len and \
            len(self._kelly_cap) == deque_len and \
            len(self._asset) == deque_len and \
            len(self._cash) == deque_len and \
            len(self._borrowed_cash) == deque_len and \
            len(self._target_cash) == deque_len and \
            len(self._cash_asset) == deque_len and \
            len(self._realized_pnl) == deque_len and \
            len(self._realized_pnl_pct) == deque_len and \
            len(self._fee1) == deque_len and \
            len(self._fee2) == deque_len and \
            len(self._executed_qty) == deque_len and \
            len(self._executed_amt) == deque_len and \
            len(self._paper_pnl) == deque_len and \
            len(self._paper_pnl_pct) == deque_len and \
            len(self._total_position) == deque_len and \
            len(self._total_amt_bought) == deque_len and \
            len(self._drawdown) == deque_len and \
            len(self._drawdown_pct) == deque_len and \
            len(self._cumulative_realized_pnl) == deque_len, \
            "data length does not match" \
            f"_kelly_p: {len(self._kelly_p)}, " \
            f"_kelly_b_win: {len(self._kelly_b_win)}, " \
            f"_kelly_b_loss: {len(self._kelly_b_loss)}, " \
            f"_kelly_cap: {len(self._kelly_cap)} "

        # 結束
        return self._idx

    def cal_paper_pnl_run(self, price: Decimal):
        """
        這個在還沒有 step 之前如果需要計算 paper_pnl, 就必須用這個
        """
        asset = self.silo.get_total_pos() * price
        total_amt = self.silo.get_total_amt_bought()
        if asset >= 0:
            paper_pnl = asset - total_amt
        else:
            paper_pnl = d_abs(total_amt) - d_abs(asset)

        paper_pnl_pct = d_round(paper_pnl / d_abs(total_amt), 5) if d_abs(total_amt) > 0 else d(0.)

        return paper_pnl, paper_pnl_pct


    def get_kelly_cap(self) -> float:
        return self._kelly_cap[-1]

    def get_last_kelly_cap(self) -> float:
        return self._kelly_cap[-2]

    def set_price(self,
                  dt_idx: dt.datetime,
                  price: Decimal,
                  app_env: AppEnv = AppEnv.TRAIN,
                  unit_test: bool = True):

        self.price_ary[-1] = [price]
        self._dt_idx[-1] = dt_idx


    def set_paper_pnl_pct(self, paper_pnl_pct: Decimal):
        self._paper_pnl_pct[-1] = paper_pnl_pct

    def copy_last_paper_pnl(self):
        self._paper_pnl[-1] = self._paper_pnl[-2]
        self._paper_pnl_pct[-1] = self._paper_pnl_pct[-2]

    def set_kelly(self,
                  kelly_cap: float,
                  kelly_p: float,
                  kelly_b_win: float,
                  kelly_b_loss: float):

        self._kelly_p[-1] = kelly_p
        self._kelly_cap[-1] = kelly_cap
        self._kelly_b_win[-1] = kelly_b_win
        self._kelly_b_loss[-1] = kelly_b_loss

    def print_last_10_kelly(self, idx: int):
        for i in range(11):
            index = idx - 10 + i
            self.logger.info(f"kelly_cap:{self._kelly_cap[index]:.2f} | "
                             f"p:{self._kelly_p[index]:.5f} | "
                             f"win:{self._kelly_b_win[index]:.5f} | "
                             f"loss:{self._kelly_b_loss[index]:.5f}")

    def inc_num_trades(self):
        """
        增加一次交易次數
        """
        self.num_trade += 1

    def append_profit_trade(self, is_profit: bool):
        """
        用來紀錄是否賺錢
        """
        pass

    def is_significant_pos_now(self):
        """
        Check the latest position after execution
        """
        if self.get_position() is None:
            raise Exception("is_significant_pos_now.get_position() cannot be None")
        amt = self.get_position() * self.get_price()[0]
        return d_abs(amt) > self.min_notional * d(1.5)

    def is_significant_pos(self) -> int:
        """
        Check if position left is significant, it is useful when silo is only 1
        1 if long position
        -1 if short position
        0 if no significant position
        """
        a = self.get_last_price()
        b = self.get_last_position()
        amt = self.get_last_position() * self.get_last_price()[0]
        # print(f"amt:{amt}, min_notional * 1.5 = {self.min_notional * d(1.5)}")
        if d_abs(amt) > self.min_notional * d(1.5) and amt > 0:
            return 1
        elif d_abs(amt) > self.min_notional * d(1.5) and amt < 0:
            return -1
        else:
            return 0

    def ds_cal_kelly_cap(self):

        def reject_outliers(data, m=2.):
            _d = np.abs(data - np.median(data))
            m_dev = np.median(_d)
            s = _d / m_dev if m_dev else d(0.)
            return np.concatenate([data[s < m], data[s < m] * d(0.8)])

        try:
            idx = self._idx

            realized_pnl_pct_concat = np.hstack((self._realized_pnl_pct_hist, self._realized_pnl_pct))
            current_cur = self._realized_pnl_pct_hist.size + idx

            period = int(int(convert_to_min(self.kelly_cap_args["must_trade_max"]) * 10
                             / convert_to_min(self.kelly_cap_args["trade_interval"])))
            period_start = max(current_cur - period, 0)
            period_end = current_cur  # + 1

            # if period_start == 0:
            #    return round(self.KellyCls.kelly_cap, 3)

            # count what is inside the history too
            # pnl_ary_hist = self._realized_pnl_pct_hist[period_start:period_end] if self._realized_pnl_pct_hist is not None else np.array([0,])
            # pnl_ary = self._realized_pnl_pct[period_start:period_end]
            pnl_ary = realized_pnl_pct_concat[period_start:period_end]

            win = pnl_ary[pnl_ary > 0]
            loss = pnl_ary[pnl_ary < 0]

            # special case, if there is empty case, then just put in one
            if len(loss) == 0 and len(win) > 0:
                loss = np.array([d(np.mean(win)) / d(3)])
            elif len(loss) == 0 and len(win) == 0:
                win = np.array([d(0.001)])
                loss = np.array([d(0.001)])

            if len(win) == 0:
                win = np.array([d(0.001)])

            # win_normal = reject_outliers(win)
            # loss_normal = reject_outliers(loss)
            # i want outlier now
            win_normal = win
            loss_normal = loss

            win_normal = [d(v) for v in win_normal]
            loss_normal = [d(v) for v in loss_normal]
            b_win = np.mean(win_normal)
            b_loss = d_abs(np.mean(loss_normal))

            # self.logger.info(f"[BacktestOrderData] win mean: {b_win*100:.2f}%, loss mean:{b_loss*100:.2f}%\n"
            #                   f"win: {win}\n"
            #                   f"loss: {loss}\n")

            # if np.isnan(b_loss).any() or np.isnan(b_win).any():
            # if np.any(np.vectorize(lambda x: x.is_nan())(np.asarray(b_loss))) or \
            #         np.any(np.vectorize(lambda x: x.is_nan())(np.asarray(b_win))):
            #    raise Exception(f"b_win: {b_win:.5f} b_loss:{b_loss:.5f} cannot be Nan")

            # use original to log num of win/loss
            if len(win) + len(loss) > d(3):
                # we have to accumulate enough cases, or we still use the default 0.51
                p = len(win) / (len(win) + len(loss))
            else:
                p = 0.505

            self.KellyCls.update_p(p)
            self.KellyCls.update_b(b_win, b_loss)
            kelly_cap = self.KellyCls.cal_kelly_cap()

            # self.logger.info(f"[BacktestOrderData] kelly_cap: {kelly_cap}, Prob {p*100:.2f}%, win mean: {b_win*100:.2f}%, loss mean:{b_loss*100:.2f}%\n"
            #                   f"win: {win}\n"
            #                   f"loss: {loss}\n")

            return kelly_cap

        except Exception as e:
            err = readable_error(e, __file__)
            self.logger.debug(f"[BacktestOrderData] self._realized_pnl_pct: {self._realized_pnl_pct}")
            self.logger.error(err)
            time.sleep(3)
            sys.exit()

    def reset_kelly_cal(self):
        self.KellyCls = Kelly()

        # logging
        self._kelly_p = deque([None], maxlen=self.max_deque)
        self._kelly_b_win = deque([None], maxlen=self.max_deque)
        self._kelly_b_loss = deque([None], maxlen=self.max_deque)
        self._kelly_cap = deque([None], maxlen=self.max_deque)

        # assign initial values
        # 移到 env.reset 去做 looping init
        # for i in range(idx + 1):
        #     self._kelly_p.append(self.KellyCls.p)
        #     self._kelly_b_win.append(self.KellyCls.b_win)
        #     self._kelly_b_loss.append(self.KellyCls.b_loss)
        #     self._kelly_cap.append(self.ds_cal_kelly_cap())

