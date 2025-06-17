import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import pandas as pd
from database.database_manager import DatabaseManager

def main():
    db_manager = DatabaseManager()
    with db_manager:
        indicators_df = db_manager.get_all_technical_indicators()
        print('Columns:', indicators_df.columns.tolist())
        for fast, slow, sig in [(6, 13, 5), (12, 26, 9), (19, 39, 9)]:
            macd_col = f'macd_{fast}_{slow}_{sig}'
            macd_signal_col = f'macd_signal_{fast}_{slow}_{sig}'
            print(f'\nChecking {macd_col} and {macd_signal_col}')
            if macd_col in indicators_df.columns and macd_signal_col in indicators_df.columns:
                print(indicators_df[[macd_col, macd_signal_col]].head(10))
                print(f"NaN count {macd_col}: {indicators_df[macd_col].isna().sum()}")
                print(f"NaN count {macd_signal_col}: {indicators_df[macd_signal_col].isna().sum()}")
            else:
                print(f"Missing columns for {macd_col} or {macd_signal_col}")

if __name__ == "__main__":
    main()
