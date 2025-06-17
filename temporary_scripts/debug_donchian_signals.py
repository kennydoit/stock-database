import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import pandas as pd
from database.database_manager import DatabaseManager

def main():
    db_manager = DatabaseManager()
    with db_manager:
        indicators_df = db_manager.get_all_technical_indicators()
        prices_df = db_manager.get_all_stock_prices()
        # Merge to get close price
        merged_df = indicators_df.merge(
            prices_df[['symbol_id', 'date', 'close']],
            on=['symbol_id', 'date'],
            how='left'
        )
        for w in [10, 20, 50]:
            high_col = f'donchian_high_{w}'
            low_col = f'donchian_low_{w}'
            print(f'\nDonchian window: {w}')
            print(merged_df[[
                'symbol', 'date', 'close', high_col, low_col
            ]].tail(20))
            print('Rows where close > high:', (merged_df['close'] > merged_df[high_col].shift(1)).sum())
            print('Rows where close == high:', (merged_df['close'] == merged_df[high_col].shift(1)).sum())
            print('Rows where close < low:', (merged_df['close'] < merged_df[low_col].shift(1)).sum())
            print('Rows where close == low:', (merged_df['close'] == merged_df[low_col].shift(1)).sum())

if __name__ == "__main__":
    main()
