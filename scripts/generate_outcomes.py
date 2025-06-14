import sys
from pathlib import Path

# Add src and database to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))
sys.path.append(str(Path(__file__).parent.parent / 'database'))

from database_manager import DatabaseManager
from outcomes import generate_outcomes

def main():
    db_manager = DatabaseManager()
    with db_manager:
        prices_df = db_manager.get_all_stock_prices()
        if prices_df.empty:
            print("No price data found.")
            return
        outcomes_df = generate_outcomes(prices_df)
        db_manager.insert_outcomes(outcomes_df)
        print(f"Inserted {len(outcomes_df)} outcome rows.")

if __name__ == "__main__":
    main()