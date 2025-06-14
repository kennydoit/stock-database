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
        # Parabolic SAR
        group['psar'] = ta.trend.PSARIndicator(group['high'], group['low'], group['close']).psar()
        # Lags
        for col in ['close', 'high', 'low', 'open', 'volume']:
            for lag in [1, 2, 3, 5, 10, 20]:
                group[f'{col}_lag_{lag}'] = group[col].shift(lag)
        indicators.append(group)
    result = pd.concat(indicators)
    return result.reset_index(drop=True)

def generate_trade_signals(indicators_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate trade signals from technical indicators using common default thresholds.
    Returns a DataFrame matching the technical_trade_signals table schema.
    """
    signals = indicators_df[['symbol', 'date']].copy()

    # RSI signals
    for w in [7, 14, 30, 50]:
        col = f'rsi_{w}'
        sig_col = f'rsi_signal_{w}'
        signals[sig_col] = 0
        signals.loc[indicators_df[col] > 70, sig_col] = -1  # Overbought
        signals.loc[indicators_df[col] < 30, sig_col] = 1   # Oversold

    # Stochastic signals
    for w in [7, 10, 14, 21, 30]:
        k_col = f'stoch_k_{w}_3'
        sig_col = f'stoch_signal_{w}_3'
        signals[sig_col] = 0
        signals.loc[indicators_df[k_col] > 80, sig_col] = -1
        signals.loc[indicators_df[k_col] < 20, sig_col] = 1

    # MACD signals (cross above/below signal line)
    macd_configs = [(6, 13, 5), (12, 26, 9), (19, 39, 9)]
    for fast, slow, sig in macd_configs:
        macd_col = f'macd_{fast}_{slow}_{sig}'
        macd_signal_col = f'macd_signal_{fast}_{slow}_{sig}'
        sig_col = f'macd_cross_signal_{fast}_{slow}_{sig}'
        signals[sig_col] = 0
        # Buy: MACD crosses above signal
        signals.loc[
            (indicators_df[macd_col] > indicators_df[macd_signal_col]) &
            (indicators_df[macd_col].shift(1) <= indicators_df[macd_signal_col].shift(1)),
            sig_col
        ] = 1
        # Sell: MACD crosses below signal
        signals.loc[
            (indicators_df[macd_col] < indicators_df[macd_signal_col]) &
            (indicators_df[macd_col].shift(1) >= indicators_df[macd_signal_col].shift(1)),
            sig_col
        ] = -1

    # SMA cross signals (short/long)
    for short, long in [(5, 20), (10, 50), (20, 100), (50, 200)]:
        short_col = f'sma_{short}'
        long_col = f'sma_{long}'
        sig_col = f'sma_cross_signal_{short}_{long}'
        signals[sig_col] = 0
        # Golden cross
        signals.loc[
            (indicators_df[short_col] > indicators_df[long_col]) &
            (indicators_df[short_col].shift(1) <= indicators_df[long_col].shift(1)),
            sig_col
        ] = 1
        # Death cross
        signals.loc[
            (indicators_df[short_col] < indicators_df[long_col]) &
            (indicators_df[short_col].shift(1) >= indicators_df[long_col].shift(1)),
            sig_col
        ] = -1

    # Bollinger Bands signals (price outside bands)
    for w in [10, 14, 20, 50]:
        upper = f'bb_upper_{w}'
        lower = f'bb_lower_{w}'
        price = indicators_df['close']
        sig_col = f'bb_signal_{w}'
        signals[sig_col] = 0
        signals.loc[price > indicators_df[upper], sig_col] = -1
        signals.loc[price < indicators_df[lower], sig_col] = 1

    # CCI signals
    for w in [10, 14, 20, 40]:
        col = f'cci_{w}'
        sig_col = f'cci_signal_{w}'
        signals[sig_col] = 0
        signals.loc[indicators_df[col] > 100, sig_col] = -1
        signals.loc[indicators_df[col] < -100, sig_col] = 1

    # ADX trend confirmation (not buy/sell, but trend/no trend)
    for w in [7, 14, 21, 30]:
        col = f'adx_{w}'
        sig_col = f'adx_trend_{w}'
        signals[sig_col] = (indicators_df[col] > 20).astype(int)

    # Parabolic SAR (price crosses above/below SAR)
    psar_col = 'psar'
    sig_col = 'psar_signal'
    signals[sig_col] = 0
    signals.loc[indicators_df['close'] > indicators_df[psar_col], sig_col] = 1
    signals.loc[indicators_df['close'] < indicators_df[psar_col], sig_col] = -1

    # Donchian Channel (price breaks channel)
    for w in [10, 20, 50]:
        high_col = f'donchian_high_{w}'
        low_col = f'donchian_low_{w}'
        sig_col = f'donchian_signal_{w}'
        signals[sig_col] = 0
        signals.loc[indicators_df['close'] > indicators_df[high_col], sig_col] = 1
        signals.loc[indicators_df['close'] < indicators_df[low_col], sig_col] = -1

    # You can add more or adjust as needed!

    return signals