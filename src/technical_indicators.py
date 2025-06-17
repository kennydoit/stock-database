import pandas as pd
import ta

def generate_indicators(prices_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate technical indicators for all symbols and dates using ta.
    Returns a DataFrame matching the technical_indicators table schema.
    """
    indicators = []
    for symbol, group in prices_df.groupby('symbol'):
        group = group.sort_values('date').copy()
        # RSI
        for w in [7, 14, 30, 50]:
            group[f'rsi_{w}'] = ta.momentum.RSIIndicator(group['close'], window=w).rsi()
        # SMA
        for w in [5, 10, 20, 50, 100, 200]:
            group[f'sma_{w}'] = ta.trend.SMAIndicator(group['close'], window=w).sma_indicator()
        # EMA
        for w in [5, 10, 20, 50, 100, 200]:
            group[f'ema_{w}'] = ta.trend.EMAIndicator(group['close'], window=w).ema_indicator()
        # MACD
        macd_configs = [(6, 13, 5), (12, 26, 9), (19, 39, 9)]
        for fast, slow, sig in macd_configs:
            macd = ta.trend.MACD(group['close'], window_fast=fast, window_slow=slow, window_sign=sig)
            group[f'macd_{fast}_{slow}_{sig}'] = macd.macd()
            group[f'macd_signal_{fast}_{slow}_{sig}'] = macd.macd_signal()
            group[f'macd_hist_{fast}_{slow}_{sig}'] = macd.macd_diff()
        # Bollinger Bands
        for w in [10, 14, 20, 50]:
            bb = ta.volatility.BollingerBands(group['close'], window=w)
            group[f'bb_upper_{w}'] = bb.bollinger_hband()
            group[f'bb_middle_{w}'] = bb.bollinger_mavg()
            group[f'bb_lower_{w}'] = bb.bollinger_lband()
        # Stochastic Oscillator
        for w in [7, 10, 14, 21, 30]:
            stoch = ta.momentum.StochasticOscillator(group['high'], group['low'], group['close'], window=w, smooth_window=3)
            group[f'stoch_k_{w}_3'] = stoch.stoch()
            group[f'stoch_d_{w}_3'] = stoch.stoch_signal()
        # CCI
        for w in [10, 14, 20, 40]:
            group[f'cci_{w}'] = ta.trend.CCIIndicator(group['high'], group['low'], group['close'], window=w).cci()
        # ATR
        for w in [7, 14, 21, 30]:
            group[f'atr_{w}'] = ta.volatility.AverageTrueRange(group['high'], group['low'], group['close'], window=w).average_true_range()
        # OBV
        group['obv'] = ta.volume.OnBalanceVolumeIndicator(group['close'], group['volume']).on_balance_volume()
        for w in [10, 20, 50]:
            group[f'obv_{w}'] = group['obv'].rolling(w).mean()
        # Ichimoku (standard and alternatives)
        ichimoku_configs = [(9, 26, 52), (7, 22, 52), (12, 33, 52)]
        for conv, base, span_b in ichimoku_configs:
            ichimoku = ta.trend.IchimokuIndicator(group['high'], group['low'], window1=conv, window2=base, window3=span_b)
            group[f'ichimoku_conv_{conv}'] = ichimoku.ichimoku_conversion_line()
            group[f'ichimoku_base_{base}'] = ichimoku.ichimoku_base_line()
            group[f'ichimoku_spanb_{span_b}'] = ichimoku.ichimoku_b()
        # Donchian Channel
        for w in [10, 20, 50]:
            group[f'donchian_high_{w}'] = group['high'].rolling(window=w).max()
            group[f'donchian_low_{w}'] = group['low'].rolling(window=w).min()
        # ADX
        for w in [7, 14, 21, 30]:
            group[f'adx_{w}'] = ta.trend.ADXIndicator(group['high'], group['low'], group['close'], window=w).adx()
        # Parabolic SAR (multiple parameter sets for signals)
        group['psar_001_02'] = ta.trend.PSARIndicator(group['high'], group['low'], group['close'], step=0.01, max_step=0.2).psar()
        group['psar_002_02'] = ta.trend.PSARIndicator(group['high'], group['low'], group['close'], step=0.02, max_step=0.2).psar()
        group['psar_004_02'] = ta.trend.PSARIndicator(group['high'], group['low'], group['close'], step=0.04, max_step=0.2).psar()
        # Lags
        for col in ['close', 'high', 'low', 'open', 'volume']:
            for lag in [1, 2, 3, 5, 10, 20]:
                group[f'{col}_lag_{lag}'] = group[col].shift(lag)
        indicators.append(group)
    result = pd.concat(indicators)
    return result.reset_index(drop=True)
