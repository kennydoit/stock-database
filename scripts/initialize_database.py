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
    print(f"📍 Database will be created at: {actual_path}")
    
    # Move existing database if it's in wrong location
    main_folder_db = project_root / db_name
    if main_folder_db.exists() and main_folder_db != database_path:
        print(f"📦 Moving database from {main_folder_db} to {database_path}")
        try:
            import shutil
            shutil.move(str(main_folder_db), str(database_path))
            print(f"✅ Database moved to correct location")
        except Exception as e:
            print(f"⚠️ Could not move database: {e}")
    
    # Load symbols from validated_symbols.yaml
    project_root = str(Path(__file__).parent.parent.parent.resolve())
    print(f"🌍 Project root: {project_root}")
    def expand_path(path):
        return path.replace("$project_root", project_root)

    symbols_file = Path(expand_path(config["symbols_file"]))
    
    if not symbols_file.exists():
        print(f"❌ Symbols file not found: {symbols_file}")
    else:
        print(f"📥 Loading symbols from: {symbols_file}")
        try:
            with open(symbols_file, 'r') as f:
                validated_symbols = yaml.safe_load(f)
            print(f"✅ Loaded symbols from validated_symbols.yaml")
            
            # Show what we loaded
            if isinstance(validated_symbols, dict):
                total_symbols = sum(len(symbols) if isinstance(symbols, list) else 0 
                                  for symbols in validated_symbols.values())
                print(f"📊 Found {total_symbols} symbols across {len(validated_symbols)} categories")
                
                # Show breakdown
                for category, symbols in validated_symbols.items():
                    if isinstance(symbols, list):
                        print(f"  {category}: {len(symbols)} symbols")
                    else:
                        print(f"  {category}: {type(symbols)} (unexpected format)")
            else:
                print(f"⚠️ Unexpected YAML structure: {type(validated_symbols)}")
                
        except Exception as e:
            print(f"❌ Error loading symbols file: {e}")

    with db_manager:
        # Check current state
        try:
            symbols_df = db_manager.get_symbols()
        except Exception as e:
            print(f"⚠️ Could not get symbols (expected for new database): {e}")
            symbols_df = None
        
        if symbols_df is not None and not symbols_df.empty and not force_reset:
            print(f"📊 Database already contains {len(symbols_df)} symbols")
            print("Use --reset flag to start fresh, or run collect_price_data.py to update prices")
            
            # Show current breakdown
            sector_counts = symbols_df.groupby('sector').size().sort_values(ascending=False)
            print("\nCurrent symbol breakdown:")
            for sector, count in sector_counts.items():
                print(f"  {sector}: {count} symbols")
            return
        
        if force_reset:
            print("🗑️ Clearing existing database...")
            try:
                # Try to clear tables if they exist
                if hasattr(db_manager, 'connection'):
                    cursor = db_manager.connection.cursor()
                elif hasattr(db_manager, 'conn'):
                    cursor = db_manager.conn.cursor()
                else:
                    print("⚠️ Cannot find database connection for clearing")
                    cursor = None
                
                if cursor:
                    # Clear tables if they exist
                    tables_to_clear = ['news_symbols', 'news_articles', 'stock_prices', 'symbols']
                    for table in tables_to_clear:
                        try:
                            cursor.execute(f"DELETE FROM {table}")
                            print(f"  ✅ Cleared {table}")
                        except Exception as e:
                            print(f"  ⚠️ Could not clear {table}: {e}")
                    
                    if hasattr(db_manager, 'connection'):
                        db_manager.connection.commit()
                    elif hasattr(db_manager, 'conn'):
                        db_manager.conn.commit()
                    
                    print("✅ Database cleared")
            except Exception as e:
                print(f"⚠️ Error clearing database: {e}")
        
        # Create schema if it doesn't exist
        print("🏗️ Creating database schema...")
        try:
            db_manager.setup_database()
            print("✅ Schema created")
        except Exception as e:
            print(f"⚠️ Schema creation error: {e}")
        
        # Insert symbols from validated_symbols.yaml
        print("📥 Inserting symbols from validated_symbols.yaml...")
        
        total_inserted = 0
        for sector, symbols in validated_symbols.items():
            if not isinstance(symbols, list):
                print(f"⚠️ Skipping {sector}: not a list ({type(symbols)})")
                continue
                
            print(f"📊 Adding {sector} symbols ({len(symbols)} symbols)...")
            
            # Determine market cap based on sector
            if sector.upper() == 'ETF':
                market_cap = 'ETF'
            elif 'SMALL' in sector.upper() or 'MICRO' in sector.upper():
                market_cap = 'Small'
            elif 'MID' in sector.upper():
                market_cap = 'Mid'
            else:
                market_cap = 'Large'  # Default assumption
            
            for symbol_data in symbols:
                try:
                    # Handle both string symbols and dictionary objects
                    if isinstance(symbol_data, str):
                        # Simple string symbol
                        symbol = symbol_data.upper()
                        db_manager.insert_symbol(
                            symbol=symbol,
                            sector=sector,
                            market_cap=market_cap
                        )
                    elif isinstance(symbol_data, dict):
                        # Dictionary with detailed symbol information
                        symbol = symbol_data.get('symbol', '').upper()
                        if not symbol:
                            print(f"  ⚠️ Skipping entry with no symbol: {symbol_data}")
                            continue
                            
                        # Use data from the dictionary if available
                        symbol_sector = symbol_data.get('sector', sector)
                        symbol_market_cap = symbol_data.get('market_cap', market_cap)
                        
                        # Convert numeric market cap to category
                        if isinstance(symbol_market_cap, (int, float)):
                            if symbol_market_cap > 10_000_000_000:  # > 10B
                                symbol_market_cap = 'Large'
                            elif symbol_market_cap > 2_000_000_000:  # > 2B
                                symbol_market_cap = 'Mid'
                            else:
                                symbol_market_cap = 'Small'
                        
                        db_manager.insert_symbol(
                            symbol=symbol,
                            sector=symbol_sector,
                            market_cap=symbol_market_cap
                        )
                    else:
                        print(f"  ⚠️ Unexpected symbol format: {type(symbol_data)} - {symbol_data}")
                        continue
                    
                    total_inserted += 1
                    print(f"  ✅ {symbol}")
                    
                except Exception as e:
                    symbol_name = symbol_data if isinstance(symbol_data, str) else symbol_data.get('symbol', 'Unknown')
                    print(f"  ⚠️ Warning: Could not insert {symbol_name}: {e}")
        
        # Display final summary
        try:
            symbols_df = db_manager.get_symbols()
            print(f"\n📊 Database Summary:")
            print(f"  Database location: {getattr(db_manager, 'db_path', 'default location')}")
            print(f"  Total symbols: {len(symbols_df)}")
            print(f"  Successfully inserted: {total_inserted}")
            
            if not symbols_df.empty:
                sector_counts = symbols_df.groupby('sector').size().sort_values(ascending=False)
                print(f"\nSector breakdown:")
                for sector, count in sector_counts.items():
                    print(f"  {sector}: {count} symbols")
                
                # Show key symbols for verification
                print(f"\n🎯 Key symbols verification:")
                key_symbols = ['ACN', 'AAPL', 'MSFT', 'GOOGL', 'META', 'SPY', 'QQQ']
                for symbol in key_symbols:
                    if symbol in symbols_df['symbol'].values:
                        print(f"  ✅ {symbol}")
                    else:
                        print(f"  ❌ {symbol} - MISSING")
                        
                # Show sample of what was actually inserted
                print(f"\n📋 Sample of inserted symbols:")
                sample_symbols = symbols_df['symbol'].head(10).tolist()
                print(f"  {', '.join(sample_symbols)}")
                if len(symbols_df) > 10:
                    print(f"  ... and {len(symbols_df) - 10} more")
            
        except Exception as e:
            print(f"⚠️ Could not retrieve final summary: {e}")
            print(f"📊 Attempted to insert {total_inserted} symbols")
        
        print(f"\n✅ Database initialization complete!")
        print(f"\nNext steps:")
        print(f"  1. Run: python scripts/collect_price_data.py")
        # print(f"  2. Run: python scripts/train_enhanced_model.py --symbol ACN")
        print(f"\n📁 Symbols loaded from: {symbols_file}")

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