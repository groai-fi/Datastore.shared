"""Enums for Binance trading module"""
from enum import Enum, IntEnum


class TradeAction(IntEnum):
    """Trading actions"""
    HOLD = 0
    BUY = 1
    SELL = 2
    SHORT = 3
    COVER = 4


class ErrorCodes(IntEnum):
    """Error codes for trading"""
    ERROR_TH001 = 1001
    ERROR_TH002 = 1002
    ERROR_TH004 = 1004
    ERROR_TH005 = 1005
    ERROR_TH006 = 1006
    ERROR_TH008 = 1008
    ERROR_TH009 = 1009
    ERROR_TH011 = 1011


class AppEnv(Enum):
    """Application environment"""
    TRAIN = "train"
    TRADE = "trade"
    BACKTEST = "backtest"


class OrderStatus(Enum):
    """Order status"""
    NEW = "NEW"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"
