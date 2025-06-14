import pandas as pd

LOOKAHEADS = [1, 3, 5, 7, 10, 14, 21, 28, 60, 90, 120]

def generate_outcomes(prices_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each symbol_id and date, compute look-ahead prices and returns.
    Returns a DataFrame matching the outcomes table schema.
    """
    outcomes = []
    for symbol_id, group in prices_df.groupby('symbol_id'):
        group = group.sort_values('date').copy()
        for d in LOOKAHEADS:
            group[f'price_d{d}'] = group['close'].shift(-d)
            group[f'returns_d{d}'] = (group[f'price_d{d}'] - group['close']) / group['close']
        outcomes.append(group[['symbol_id', 'date'] + [f'price_d{d}' for d in LOOKAHEADS] + [f'returns_d{d}' for d in LOOKAHEADS]])
    result = pd.concat(outcomes)
    return result.reset_index(drop=True)