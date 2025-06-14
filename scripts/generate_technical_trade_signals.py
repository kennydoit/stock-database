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
        if indicators_df.empty:
            print("No technical indicators found. Run generate_technical_indicators.py first.")
            return

        # Generate trade signals
        signals_df = generate_trade_signals(indicators_df)

        # Insert signals into technical_trade_signals table
        db_manager.insert_technical_trade_signals(signals_df)
        print(f"Inserted {len(signals_df)} trade signal rows.")

if __name__ == "__main__":
    main()