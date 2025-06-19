import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import pandas as pd
from database.database_manager import DatabaseManager

def main():
    db_manager = DatabaseManager()
    with db_manager:
        df = db_manager.get_all_technical_indicators()
        print('First 5 rows:')
        print(df[['date']].head())
        print('dtypes:')
        print(df.dtypes['date'])
        print('Unique date types:', set(type(x) for x in df['date'].head(20)))

if __name__ == "__main__":
    main()
