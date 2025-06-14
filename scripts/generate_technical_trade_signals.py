import sys
from pathlib import Path
import pandas as pd

# Add src and database to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))
sys.path.append(str(Path(__file__).parent.parent / 'database'))

from database_manager import DatabaseManager
from technical_trade_signals import generate_trade_signals

def main():
    db_manager = DatabaseManager()
    with db_manager:
        # Load all technical indicators
        indicators_df = db_manager.get_all_technical_indicators()
        prices_df = db_manager.get_all_stock_prices()

        # Ensure indicators_df has 'symbol' column for signal generation
        if 'symbol' not in indicators_df.columns:
            symbol_map = prices_df[['symbol_id', 'symbol']].drop_duplicates()
            indicators_df = indicators_df.merge(symbol_map, on='symbol_id', how='left')

        # Merge on symbol_id and date to get price/close
        merged_df = indicators_df.merge(
            prices_df[['symbol_id', 'date', 'close']],
            on=['symbol_id', 'date'],
            how='left'
        )

        signals_df = generate_trade_signals(merged_df)

        # Merge to get symbol_id from symbol (if not present)
        if 'symbol_id' not in signals_df.columns:
            symbol_map = prices_df[['symbol', 'symbol_id']].drop_duplicates()
            signals_df = signals_df.merge(symbol_map, on='symbol', how='left')
        # Drop symbol column, keep symbol_id
        if 'symbol' in signals_df.columns:
            signals_df = signals_df.drop(columns=['symbol'])
        # Drop rows with missing symbol_id
        signals_df = signals_df.dropna(subset=['symbol_id'])
        signals_df['symbol_id'] = signals_df['symbol_id'].astype(int)

        # Insert signals into technical_trade_signals table
        db_manager.insert_technical_trade_signals(signals_df)
        print(f"Inserted {len(signals_df)} trade signal rows.")

if __name__ == "__main__":
    main()