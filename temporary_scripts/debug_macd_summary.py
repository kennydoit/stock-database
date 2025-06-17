import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import pandas as pd
from database.database_manager import DatabaseManager

def main():
    db_manager = DatabaseManager()
    with db_manager:
        indicators_df = db_manager.get_all_technical_indicators()
        for fast, slow, sig in [(6, 13, 5), (12, 26, 9), (19, 39, 9)]:
            macd_col = f'macd_{fast}_{slow}_{sig}'
            macd_signal_col = f'macd_signal_{fast}_{slow}_{sig}'
            print(f'\nSummary for {macd_col} and {macd_signal_col}')
            print(indicators_df[[macd_col, macd_signal_col]].describe())
            print('First 20 rows:')
            print(indicators_df[[macd_col, macd_signal_col]].head(20))
            print('Last 20 rows:')
            print(indicators_df[[macd_col, macd_signal_col]].tail(20))
            print(f'Non-NaN count {macd_col}:', indicators_df[macd_col].notna().sum())
            print(f'Non-NaN count {macd_signal_col}:', indicators_df[macd_signal_col].notna().sum())

if __name__ == "__main__":
    main()
