import pandas as pd

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
        signals.loc[indicators_df[col] > 70, sig_col] = -1
        signals.loc[indicators_df[col] < 30, sig_col] = 1

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
        signals.loc[
            (indicators_df[macd_col] > indicators_df[macd_signal_col]) &
            (indicators_df[macd_col].shift(1) <= indicators_df[macd_signal_col].shift(1)),
            sig_col
        ] = 1
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
        signals.loc[
            (indicators_df[short_col] > indicators_df[long_col]) &
            (indicators_df[short_col].shift(1) <= indicators_df[long_col].shift(1)),
            sig_col
        ] = 1
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

    # ADX signals (trend confirmation: 1 if ADX > 20, else 0)
    for w in [7, 14, 21, 30]:
        col = f'adx_{w}'
        sig_col = f'adx_signal_{w}'
        signals[sig_col] = (indicators_df[col] > 20).astype(int)

    # Parabolic SAR (for each PSAR column in the schema, only if present)
    psar_columns = [col for col in ["psar_001_02", "psar_002_02", "psar_004_02"] if col in indicators_df.columns]
    for psar_col in psar_columns:
        sig_col = f"psar_signal_{psar_col.split('_')[1]}_{psar_col.split('_')[2]}"
        signals[sig_col] = 0
        # Fill None/NaN with a value that will never trigger a signal (e.g., -inf for PSAR, inf for close)
        close = indicators_df['close'].astype(float).fillna(float('-inf'))
        psar = indicators_df[psar_col].astype(float).fillna(float('inf'))
        signals.loc[close > psar, sig_col] = 1
        signals.loc[close < psar, sig_col] = -1

    # Donchian Channel (price breaks previous channel)
    for w in [10, 20, 50]:
        high_col = f'donchian_high_{w}'
        low_col = f'donchian_low_{w}'
        sig_col = f'donchian_signal_{w}'
        signals[sig_col] = 0
        signals.loc[indicators_df['close'] > indicators_df[high_col].shift(1), sig_col] = 1
        signals.loc[indicators_df['close'] < indicators_df[low_col].shift(1), sig_col] = -1

    return signals