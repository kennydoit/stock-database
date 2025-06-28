#!/usr/bin/env python3
"""
Initialize database and populate with universe symbols
Provides options to start completely fresh or use existing configurations
"""
import sys
from pathlib import Path
import yaml
import logging

# Add database path
sys.path.append(str(Path(__file__).parent.parent / 'database'))
from database_manager import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config.yaml
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

def initialize_database(force_reset=False, db_name="stock_database.db"):
    """Initialize database and populate with universe symbols"""
    
    print("🗄️ Initializing Historical Stock Data Database")
    print("="*55)
    print(f"📁 Database name: {db_name}")
    
    # Ensure database goes in the /database folder
    project_root = Path(__file__).parent.parent
    database_dir = project_root / 'database'
    database_path = database_dir / db_name
    
    print(f"🎯 Target database location: {database_path}")
    
    # Create database directory if it doesn't exist
    database_dir.mkdir(exist_ok=True)
    
    # Setup database - check if DatabaseManager accepts custom db path
    try:
        # Try different initialization patterns
        if hasattr(DatabaseManager, '__init__'):
            import inspect
            sig = inspect.signature(DatabaseManager.__init__)
            params = list(sig.parameters.keys())
            print(f"🔍 DatabaseManager parameters: {params}")
            
            # Try common parameter names with full path
            if 'db_path' in params:
                db_manager = DatabaseManager(db_path=str(database_path))
            elif 'database_path' in params:
                db_manager = DatabaseManager(database_path=str(database_path))
            elif 'db_file' in params:
                db_manager = DatabaseManager(db_file=str(database_path))
            elif 'path' in params:
                db_manager = DatabaseManager(path=str(database_path))
            else:
                # No custom path parameter, use default and modify after
                db_manager = DatabaseManager()
                # Try to set the database path after initialization
                if hasattr(db_manager, 'db_path'):
                    db_manager.db_path = database_path
                    print(f"📍 Set db_path to: {db_manager.db_path}")
                elif hasattr(db_manager, 'database_path'):
                    db_manager.database_path = database_path
                    print(f"📍 Set database_path to: {db_manager.database_path}")
                elif hasattr(db_manager, 'path'):
                    db_manager.path = database_path
                    print(f"📍 Set path to: {db_manager.path}")
                else:
                    print("⚠️ Cannot set custom database path - using default location")
        else:
            db_manager = DatabaseManager()
            
    except Exception as e:
        print(f"⚠️ DatabaseManager initialization issue: {e}")
        print("🔄 Using default DatabaseManager initialization")
        db_manager = DatabaseManager()
        
        # Try to override the path after creation
        try:
            if hasattr(db_manager, 'db_path'):
                db_manager.db_path = database_path
            elif hasattr(db_manager, 'database_path'):
                db_manager.database_path = database_path
        except Exception as path_error:
            print(f"⚠️ Could not set custom path: {path_error}")
    
    # Verify where the database will actually be created
    actual_path = getattr(db_manager, 'db_path', 
                         getattr(db_manager, 'database_path', 
                                getattr(db_manager, 'path', 'unknown')))
    print(f"\U0001F4CD Database will be created at: {actual_path}")

    # --- NEW: Skip YAML, use symbols from DB ---
    with db_manager:
        # Create schema if it doesn't exist
        print("\U0001F3D7️ Creating database schema...")
        try:
            db_manager.setup_database()
            print("✅ Schema created")
        except Exception as e:
            print(f"⚠️ Schema creation error: {e}")

        # Fetch and display symbols from the database
        try:
            symbols_df = db_manager.get_symbols()
            if symbols_df is not None and not symbols_df.empty:
                print(f"\U0001F4CA Database contains {len(symbols_df)} symbols:")
                sector_counts = symbols_df.groupby('sector').size().sort_values(ascending=False)
                print("\nSector breakdown:")
                for sector, count in sector_counts.items():
                    print(f"  {sector}: {count} symbols")
                print("\n\U0001F3AF Key symbols verification:")
                key_symbols = ['ACN', 'AAPL', 'MSFT', 'GOOGL', 'META', 'SPY', 'QQQ']
                for symbol in key_symbols:
                    if symbol in symbols_df['symbol'].values:
                        print(f"  ✅ {symbol}")
                    else:
                        print(f"  ❌ {symbol} - MISSING")
                print("\n\U0001F4CB Sample of symbols:")
                sample_symbols = symbols_df['symbol'].head(10).tolist()
                print(f"  {', '.join(sample_symbols)}")
                if len(symbols_df) > 10:
                    print(f"  ... and {len(symbols_df) - 10} more")
            else:
                print("⚠️ No symbols found in the database.")
        except Exception as e:
            print(f"⚠️ Could not retrieve symbols: {e}")

    print(f"\n✅ Database initialization complete!")
    print(f"\nNext steps:")
    print(f"  1. Run: python scripts/collect_price_data.py")
    # print(f"  2. Run: python scripts/train_enhanced_model.py --symbol ACN")

def main():
    """Main function with command line argument support"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Initialize stock prediction database')
    parser.add_argument('--reset', action='store_true', 
                       help='Force reset - clear existing data and start fresh')
    parser.add_argument('--db-name', default='stock_database.db',
                       help='Database name (default: stock_database.db)')
    
    args = parser.parse_args()
    
    if args.reset:
        confirm = input("⚠️ This will delete ALL existing data. Continue? (y/N): ")
        if confirm.lower() != 'y':
            print("❌ Cancelled")
            return
    
    initialize_database(force_reset=args.reset, db_name=args.db_name)

if __name__ == "__main__":
    main()