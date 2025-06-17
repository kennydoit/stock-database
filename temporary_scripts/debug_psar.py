import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import pandas as pd
from database.database_manager import DatabaseManager
import ta

def main():
    db_manager = DatabaseManager()
    with db_manager:
        prices_df = db_manager.get_all_stock_prices()
        for symbol, group in prices_df.groupby('symbol'):
            group = group.sort_values('date').copy()
            print(f'\nSymbol: {symbol}, Rows: {len(group)}')
            print(group[['date', 'high', 'low', 'close']].head(5))
            try:
                group['psar_001_02'] = ta.trend.PSARIndicator(group['high'], group['low'], group['close'], step=0.01, max_step=0.2).psar()
                print('First 10 psar_001_02:', group['psar_001_02'].head(10).tolist())
                print('NaN count:', group['psar_001_02'].isna().sum())
            except Exception as e:
                print(f'Error calculating PSAR for {symbol}: {e}')

if __name__ == "__main__":
    main()
