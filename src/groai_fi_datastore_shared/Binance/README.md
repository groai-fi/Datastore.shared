


## 使用層次關係圖
```
├── BinanceSmartOrder @BinanecTrade
│   └── Binance[Order]
│       └── Binance[Client]
│           ├── binance.client (外部library)
│           └── BinanceClient (Deprecated)
├── Backtest[Order] (parent BaseOrder)
│   └── Binance[Order]
├── Binance[MarketData]
│   ├── Binance[Order]
│   └── Binance[MarketDataDownload]
├── ...
├── ...
└── ...
```

## 外部使用關係圖

```
├── /Trade/Binance/Binance[Trade].py
│   └── Binance[SmartOrder] @BinanecTrade
│       └── BinanceOrder
├── /envs/XXX_Envs.py
│   └── Backtest[Order]
├── /Trade/Binance/DataCatchup.py
│   └── Binance[MarketDataDownloader]
├── /Gen_Indicator.py
│   └── Binance[MarketDataDownloader]
├── tests/order/*
│   └── Backtest[Order]
├── ...
└── ...
```