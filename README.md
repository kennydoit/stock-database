## technical_trade_signals Table

| Column Name                  | Description                                      | Default Buy Signal        | Default Sell Signal        |
|------------------------------|--------------------------------------------------|---------------------------|---------------------------|
| rsi_signal_{w}               | RSI signal for window w                          | RSI < 30                  | RSI > 70                  |
| stoch_signal_{w}_3           | Stochastic %K signal for window w                | %K < 20                   | %K > 80                   |
| macd_cross_signal_{f}_{s}_{g}| MACD cross above/below signal line               | MACD crosses above signal | MACD crosses below signal |
| sma_cross_signal_{s}_{l}     | SMA short/long cross                             | Short crosses above long  | Short crosses below long  |
| bb_signal_{w}                | Bollinger Bands signal for window w              | Price < lower band        | Price > upper band        |
| cci_signal_{w}               | CCI signal for window w                          | CCI < -100                | CCI > 100                 |
| adx_trend_{w}                | ADX trend confirmation for window w              | ADX > 20 (trend)          | ADX <= 20 (no trend)      |
| psar_signal                  | Parabolic SAR signal                             | Price > SAR               | Price < SAR               |
| donchian_signal_{w}          | Donchian channel breakout for window w           | Price > upper channel     | Price < lower channel     |

**Note:** All thresholds are configurable in code. See `src/technical_indicators.py` for details.