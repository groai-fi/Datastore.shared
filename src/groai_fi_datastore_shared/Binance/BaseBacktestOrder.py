

class BaseBacktestOrder:
    def __init__(self, data_store, exch_order, app_env, min_notional, min_qty, logger):
        self.ds = data_store
        self.app_env = app_env
        self.min_notional = min_notional
        self.min_qty = min_qty
        self.exch_order = exch_order
        self.logger = logger

    def reset(self):
        pass

    @staticmethod
    def update_value(src, dst):
        if src is None:
            src = dst
        return src

    def hold(self, idx, dt_idx, price):
        pass

    def buy(self, idx, dt_idx, kelly_cap, price):
        pass

    def sell(self, idx, dt_idx, action, price, debug=False):
        pass

    def cover(self, idx, dt_idx, action, price, debug=False):
        pass

    def short(self, idx, dt_idx, kelly_cap, price, debug=False):
        pass
