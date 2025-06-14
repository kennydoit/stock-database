import sys
from pathlib import Path
import pandas as pd

# Add src and database to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))
sys.path.append(str(Path(__file__).parent.parent / 'database'))

from database_manager import DatabaseManager
from technical_indicators import generate_indicators

# SYMBOLS_TO_USE = ['AAPL', 'MSFT', 'GOOG']  # Example subset
SYMBOLS_TO_USE = None

def main(update_mode=False):
    db_manager = DatabaseManager()
    with db_manager:
        if update_mode:
            prices_df = db_manager.get_recent_stock_prices(lookback_days=100)
        else:
            prices_df = db_manager.get_all_stock_prices()
        if prices_df.empty:
            print("No stock price data found. Run collect_price_data.py first.")
            return

        # Filter to subset for development
        if SYMBOLS_TO_USE is not None:
            prices_df = prices_df[prices_df['symbol'].isin(SYMBOLS_TO_USE)]

        indicators_df = generate_indicators(prices_df)
        db_manager.insert_technical_indicators(indicators_df, upsert=update_mode)
        print(f"Inserted/updated {len(indicators_df)} technical indicator rows.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--update', action='store_true', help='Only update recent data (last 100 days)')
    args = parser.parse_args()
    main(update_mode=args.update)