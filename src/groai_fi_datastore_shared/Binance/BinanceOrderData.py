import os
import sys

import numpy as np
import datetime
from typing import Deque
from collections import deque
from typing import Optional, List
from decimal import Decimal
from .enums import TradeAction
from .utils import d

"""
trade_cols = ['dt_idx', 'idx_trade', 'price', 'position', 'buysell',
              'executed_qty', 'executed_amt', 'fee1', 'fee2', 'paper_pnl',
              'cash', 'borrowed_cash', 'target_cash', 'asset',
              'cash_asset', 'realized_pnl',
              'kelly_cap', 'kelly_cap_short',
              'total_position', 'total_amt_bought',
              'silo', 'orderId']
              
trade_cols_type = {
    'dt_idx': str,
    'idx_trade': int,
    'price': float,
    'position': int,
    'buysell': str,
    'executed_qty': float,
    'executed_amt': float,
    'fee1': float,
    'fee2': float,
    'paper_pnl': float,
    'cash': float,
    'borrowed_cash': float, 
    'target_cash': float,
    'asset': float,
    'cash_asset': float,
    'realized_pnl': float,
    'kelly_cap': float,
    'kelly_cap_short': float,
    'total_position': float,
    'total_amt_bought': float,
    'silo': str,
    'orderId': str,
}
"""


class BinanceOrderData:
    trade_cols = ['dt_idx', 'idx_trade', 'price', 'position', 'buysell',
                  'cash', 'borrowed_cash', 'asset',
                  'cash_asset', 'realized_pnl', 'kelly_cap',
                  'silo_pos', 'silo_amt',
                  'cumulative_realized_pnl', 'drawdown', 'drawdown_pct', 'target_cash',
                  'executed_qty', 'executed_amt', 'fee1', 'fee2', 'paper_pnl',
                  'total_position', 'total_amt_bought', 'buysell_lvl', 'trade_cash','orderId']

    trade_cols_type = {
        'dt_idx': str,
        'idx_trade': int,
        'price': float,
        'position': float,
        'buysell': str,
        'cash': float,
        'borrowed_cash': float,
        'asset': float,
        'cash_asset': float,
        'realized_pnl': float,
        'kelly_cap': float,
        'silo_pos': str,
        'silo_amt': str,
        'cumulative_realized_pnl': float,
        'drawdown': float,
        'drawdown_pct': float,
        'target_cash': float,
        'executed_qty': float,
        'executed_amt': float,
        'fee1': float, 'fee2': float,
        'paper_pnl': float,
        'total_position': float,
        'total_amt_bought': float,
        'buysell_lvl': float,
        'trade_cash': float,
        'orderId': str,
    }

    def __init__(self,
                 symbol: str,
                 home_asset: str,
                 target_asset: str,
                 logger: any):

        self.max_deque = 1000 if os.getenv("NODE_ENV") is None else 500
        self.logger = logger
        self.logger.info(f'[TradeData] {__name__} Class loaded')

        self.symbol = symbol
        self.home_asset = home_asset
        self.target_asset = target_asset

        # idx 計算
        self._idx = None

        self._dt_idx: Optional[Deque[Optional[datetime]]] = None
        self.price_ary: Optional[Deque[Optional[Decimal|List[Decimal]]]] = None
        # self.price_ary_T = None

        # storing init data for training
        self._dt_idx_init = None
        self.price_ary_init = None

        self._position: Optional[Deque[Optional[Decimal]]] = None
        self._buysell: Optional[Deque[Optional[TradeAction]]] = None
        self._asset: Optional[Deque[Optional[Decimal]]] = None
        self._cash: Optional[Deque[Optional[Decimal]]] = None
        self._borrowed_cash: Optional[Deque[Optional[Decimal]]] = None
        self._target_cash: Optional[Deque[Optional[Decimal]]] = None
        self._cash_asset: Optional[Deque[Optional[Decimal]]] = None

        self._realized_pnl: Optional[Deque[Optional[Decimal]]] = None
        self._realized_pnl_pct: Optional[Deque[Optional[Decimal]]] = None

        # used to load history data into here and hstack with others
        self._realized_pnl_pct_hist = np.zeros(1)
        self._cumulative_realized_pnl = None

        self._fee1: Optional[Deque[Optional[Decimal]]] = None
        self._fee2: Optional[Deque[Optional[Decimal]]] = None
        self._executed_qty: Optional[Deque[Optional[Decimal]]] = None
        self._executed_amt: Optional[Deque[Optional[Decimal]]] = None
        self._paper_pnl: Optional[Deque[Optional[Decimal]]] = None
        self._paper_pnl_pct: Optional[Deque[Optional[Decimal]]] = None
        self._total_position: Optional[Deque[Optional[Decimal]]] = None
        self._total_amt_bought: Optional[Deque[Optional[Decimal]]] = None

        self._silo_pos = deque([None])
        self._silo_amt = deque([None])

        self._trade_cash: Optional[Deque[Optional[Decimal]]] = None
        self._orderId: Optional[Deque[str]] = None
        self._drawdown: Optional[Deque[Optional[Decimal]]] = None
        self._drawdown_pct: Optional[Deque[Optional[Decimal]]] = None

        self.init_trade_cash: Optional[Decimal] = None
        self.init_target_cash: Optional[Decimal] = None

        # call at child
        # self.reset()

    def reset(self):
        """
        reset 所有的資料參數，預設是 0 也就是只有一個數字 (init_idx + 1)
        init_idx 如果不是零，那就是 stacking_lookback 大於 零
        """
        # trading
        self._idx = 0  # NOTICE: stacking_lookback

        # 故意放空的，這樣 init 時有問題馬上會知道
        self._dt_idx = deque([None], maxlen=self.max_deque)  # dtype='datetime64[ms]'
        self.price_ary = deque([None], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        # self.price_ary_T = np.array(d_zeros, dtype=np.dtype(Decimal)

        self._position = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._buysell = deque([TradeAction.HOLD], maxlen=self.max_deque)  # dtype=TradeAction)
        self._asset = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._cash = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal))  # need init at child
        self._borrowed_cash = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._target_cash = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._cash_asset = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._realized_pnl = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._realized_pnl_pct = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)

        self._fee1 = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._fee2 = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._executed_qty = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._executed_amt = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._paper_pnl = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._paper_pnl_pct = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._total_position = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._total_amt_bought = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)

        self._silo_pos = deque([None], maxlen=self.max_deque)  # dtype=object)
        self._silo_amt = deque([None], maxlen=self.max_deque)  # dtype=object)

        self._trade_cash = deque([d(0)], maxlen=self.max_deque) # dtype=np.dtype(Decimal)
        self._orderId = deque([""], maxlen=self.max_deque)  # dtype=str)
        self._drawdown = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)
        self._drawdown_pct = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)

        self._cumulative_realized_pnl = deque([d(0)], maxlen=self.max_deque)  # dtype=np.dtype(Decimal)

    def cal_paper_pnl_run(self, price) -> (float, float):
        pass

    def get_just_executed(self):
        return {
            'price': self.price_ary[-1],
            'buysell': self._buysell[-1].name,
            'position': round(self._position[-1], 8),
            'cash': round(self._cash[-1], 8),
            'borrowed_cash': round(self._borrowed_cash[-1], 8),
            'asset': round(self._asset[-1], 8),
            'cash_asset': round(self._cash_asset[-1], 8),
            'target_cash': round(self._target_cash[-1], 8),
            'executed_qty': round(self._executed_qty[-1], 8),
            'executed_amt': round(self._executed_amt[-1], 8),
            'fee1': round(self._fee1[-1], 8),
            'fee2': round(self._fee2[-1], 8),
            'paper_pnl': round(self._paper_pnl[-1], 8),
            'total_position': round(self._total_position[-1], 8),
            'total_amt_bought': round(self._total_amt_bought[-1], 8),
            'trade_cash': round(self._trade_cash[-1], 8),
            'orderId': self._orderId[-1],
        }

    def as_dict(self, child_dict=None):
        if child_dict is None:
            _d = self.__dict__.copy()
        else:
            _d = child_dict

        del _d['_dt_idx']
        del _d['_dt_idx_init']
        del _d['price_ary_init']
        del _d['_paper_pnl_pct']
        del _d['_drawdown_pct']
        del _d['price_ary']
        # del _d['tech_ary']  DEPRECATED

        del _d['_position']
        del _d['_buysell']
        del _d['_asset']
        del _d['_cash']
        del _d['_borrowed_cash']
        del _d['_cash_asset']
        del _d['_realized_pnl']
        del _d['_realized_pnl_pct']
        del _d['_cumulative_realized_pnl']
        del _d['_target_cash']

        del _d['_drawdown']
        del _d['_fee1']
        del _d['_fee2']
        del _d['_executed_qty']
        del _d['_executed_amt']
        del _d['_paper_pnl']
        del _d['_total_position']
        del _d['_total_amt_bought']
        del _d['_trade_cash']
        del _d['_orderId']  # logging

        del _d['_silo_pos']
        del _d['_silo_amt']

        del _d['logger']

        return _d

    # ============= GETTER =============
    def get_idx(self):
        return max(self._idx, 0)

    def get_last_idx(self):
        return max(self._idx - 1, 0)

    def get_dt_idx(self):
        return self._dt_idx[-1]

    def get_last_dt_idx(self):
        return self._dt_idx[-2] or self._dt_idx[-1]

    def get_price(self):
        # 在 get_state 會呼叫這個，所以可能會遇到自動要上一個，這是特例
        return self.price_ary[-1] or  self.price_ary[-2]

    def get_last_price(self):
        if self._idx == 0:
            return self.price_ary[-1]
        return self.price_ary[-2]

    def get_buysell(self):
        return self._buysell[-1]

    def get_last_buysell(self):
        return self._buysell[-2]

    def get_position(self):
        return self._position[-1]

    def get_total_position(self):
        return self._total_position[-1]

    def get_last_total_position(self):
        if self._idx == 0:
            return self._total_position[-1]
        return self._total_position[-2]

    def get_cash(self):
        return self._cash[-1]

    def get_borrowed_cash(self):
        return self._borrowed_cash[-1]

    def get_asset(self):
        return self._asset[-1]

    def get_last_asset(self):
        if self._idx == 0:
            return self._asset[-1]
        return self._asset[-2]

    def get_cash_asset(self):
        return self._cash_asset[-1]

    def get_realized_pnl(self):
        return self._realized_pnl[-1]

    def get_last_realized_pnl(self):
        return self._realized_pnl[-2]

    def get_realized_pnl_pct(self):
        return self._realized_pnl_pct[-1]

    def get_last_realized_pnl_pct(self):
        return self._realized_pnl_pct[-2]

    def get_paper_pnl(self):
        return self._paper_pnl[-1]

    def get_last_paper_pnl(self):
        return self._paper_pnl[-2]

    def get_paper_pnl_pct(self):
        # 在 get_state 會呼叫這個，所以可能會遇到自動要上一個，這是特例
        # 0 會傳回 false, 所以要檢查 is not None
        return self._paper_pnl_pct[-1] if self._paper_pnl_pct[-1] is not None else self._paper_pnl_pct[-2]

    def get_paper_pnl_pct_at(self, i):
        # 在 get_state 會呼叫這個，所以可能會遇到自動要上一個，這是特例
        return self._paper_pnl_pct[i]

    def get_last_paper_pnl_pct(self):
        return self._paper_pnl_pct[-2]

    def get_range_paper_pnl_pct(self, i):
        i = min(i, len(self._paper_pnl_pct))
        start_pt = 1
        if self._paper_pnl_pct[-1] is None:
            start_pt = 2
        return [self._paper_pnl_pct[-i] for i in range(start_pt, i)]

    def get_target_cash(self):
        return self._target_cash[-1]

    def get_last_target_cash(self):
        if self._idx == 0:
            return self._target_cash[-1]
        return self._target_cash[-2]

    def get_last_trade_cash(self):
        return self._trade_cash[-2]

    def get_trade_cash(self):
        return self._trade_cash[-1]

    def get_order_id(self):
        return self._orderId[-1]

    def get_last_position(self):
        if self._idx == 0:
            return self._position[-1]
        return self._position[-2]

    def get_last_5_position(self):
        return [self._position[-(i+1)] for i in range(min(len(self._position), 5))]

    def get_last_cash(self):
        if self._idx == 0:
            return self._cash[-1]
        return self._cash[-2]

    def get_last_cash_asset(self):
        if self._idx == 0:
            return self._cash_asset[-1]
        return self._cash_asset[-2]

    def get_drawdown(self):
        return self._drawdown[-1]

    def get_last_drawdown(self):
        return self._drawdown[-2]

    def get_drawdown_pct(self):
        return self._drawdown_pct[-1]

    def get_last_drawdown_pct(self):
        return self._drawdown_pct[-2]

    def get_last_borrowed_cash(self):
        return self._borrowed_cash[-2]

    def get_cumulative_realized_pnl(self):
        return self._cumulative_realized_pnl[-1]

    def get_last_cumulative_realized_pnl(self):
        return self._cumulative_realized_pnl[-2]

    def get_last_total_amt_bought(self):
        return self._total_amt_bought[-2]

    # ============= SETTER =============

    def set_init_cash(self, cash: Decimal):
        if len(self._cash) > 1 or len(self._cash_asset) > 1:
            raise Exception("BinanceOrderData.set_init_cash _cash or _cash_asset is not empty for init setup")

        if not isinstance(cash, Decimal):
            raise Exception(f"[BinanceOrderData] set_init_cash.cash must be Decimal got {type(cash)}")

        # self.ds.init_trade_cash = amt  # only cash initially
        self._cash[0] = cash  # only cash initially
        self._cash_asset[0] = self._cash[0] + self._asset[0]  # only cash initially

    def set_trade_cash(self, trade_cash: Decimal):
        if not isinstance(trade_cash, Decimal):
            raise Exception(f"[BinanceOrderData] set_trade_cash.trade_cash must be Decimal got {type(trade_cash)}")
        self._trade_cash[-1] = trade_cash

    def set_position(self, position: Decimal):
        self._position[-1] = position

    def set_order_id(self,
                     order_id: Optional[str],
                     _executed_qty: Decimal,
                     _executed_amt: Decimal):
        """
        交易成立後紀錄交易紀錄
        """
        self._orderId[-1] = order_id

        self._executed_amt[-1] = _executed_amt
        self._executed_qty[-1] = _executed_qty

    def set_init_target_cash(self, target_cash: Decimal):
        if len(self._cash) > 1 or len(self._target_cash) > 1:
            raise Exception("BinanceOrderData.set_init_target_cash _cash or _target_cash is not empty for init setup")

        if not isinstance(target_cash, Decimal):
            raise Exception(f"[BinanceOrderData] set_target_cash.target_cash must be Decimal got {type(target_cash)}")

        self._target_cash[-1] = target_cash

    def set_target_cash(self, target_cash: Decimal):
        if not isinstance(target_cash, Decimal):
            raise Exception(f"[BinanceOrderData] set_target_cash.target_cash must be Decimal got {type(target_cash)}")

        self._target_cash[-1] = target_cash

    def set_drawdown(self, drawdown: Decimal):
        self._drawdown[-1] = drawdown

    def set_drawdown_pct(self, drawdown_pct: Decimal):
        self._drawdown_pct[-1] = drawdown_pct

    def set_cash_asset(self, cash: Decimal, borrowed_cash: Decimal, asset: Decimal):
        self._cash[-1] = cash
        self._borrowed_cash[-1] = borrowed_cash
        self._asset[-1] = asset
        self._cash_asset[-1] = cash + borrowed_cash + asset

    def set_trade(self
                  , buysell: TradeAction
                  , position: Decimal
                  , cash: Decimal
                  , borrowed_cash: Decimal
                  , asset: Decimal):
        self._buysell[-1] = buysell
        self._position[-1] = position
        self._cash[-1] = cash
        self._borrowed_cash[-1] = borrowed_cash
        self._asset[-1] = asset
        self._cash_asset[-1] = cash + asset

    def copy_last_trade(self,
                        buysell: TradeAction=None,
                        cash: Decimal=None,
                        target_cash: Decimal=None):
        """
        方便用工具
        """
        self._buysell[-1] = self._buysell[-2] if buysell is None else buysell
        self._position[-1] = self._position[-2]
        self._cash[-1] = self._cash[-2] if cash is None else cash
        self._target_cash[-1] = self._target_cash[-2] if target_cash is None else target_cash
        self._borrowed_cash[-1] = self._borrowed_cash[-2]
        self._asset[-1] = self._asset[-2]
        self._cash_asset[-1] = self._cash[-1] + self._asset[-1]

    def set_fee1(self, fee1: Decimal):
        self._fee1[-1] = fee1

    def set_fee2(self, fee2: Decimal):
        self._fee2[-1] = fee2

    def set_cumulative_realized_pnl(self, cumulative_realized_pnl: Decimal):
        self._cumulative_realized_pnl[-1] = cumulative_realized_pnl

    def set_pnl(self
                , realized_pnl: Decimal
                , realized_pnl_pct: Decimal
                , cumulative_realized_pnl: Decimal):

        self._realized_pnl[-1] = realized_pnl
        self._realized_pnl_pct[-1] = realized_pnl_pct
        self._cumulative_realized_pnl[-1] = cumulative_realized_pnl

    def set_total(self
                  , paper_pnl: Decimal
                  , paper_pnl_pct: Decimal
                  , total_pos: Decimal
                  , total_amt_bought: Decimal):

        self._paper_pnl[-1] = paper_pnl
        self._paper_pnl_pct[-1] = paper_pnl_pct
        self._total_position[-1] = total_pos
        self._total_amt_bought[-1] = total_amt_bought

