"""
Database manager for stock prediction ML project
Handles database connections, schema creation, and basic operations
"""
import sqlite3
import pandas as pd
import logging
from pathlib import Path
# from typing import List, Dict, Optional, Tuple
from datetime import datetime, date
import yaml

logger = logging.getLogger(__name__)

TECHNICAL_INDICATOR_COLUMNS = [
    "symbol_id", "date",
    "rsi_7", "rsi_14", "rsi_30", "rsi_50",
    "sma_5", "sma_10", "sma_20", "sma_50", "sma_100", "sma_200",
    "ema_5", "ema_10", "ema_20", "ema_50", "ema_100", "ema_200",
    "macd_6_13_5", "macd_signal_6_13_5", "macd_hist_6_13_5",
    "macd_12_26_9", "macd_signal_12_26_9", "macd_hist_12_26_9",
    "macd_19_39_9", "macd_signal_19_39_9", "macd_hist_19_39_9",
    "bb_upper_10", "bb_middle_10", "bb_lower_10",
    "bb_upper_14", "bb_middle_14", "bb_lower_14",
    "bb_upper_20", "bb_middle_20", "bb_lower_20",
    "bb_upper_50", "bb_middle_50", "bb_lower_50",
    "stoch_k_7_3", "stoch_d_7_3",
    "stoch_k_10_3", "stoch_d_10_3",
    "stoch_k_14_3", "stoch_d_14_3",
    "stoch_k_21_3", "stoch_d_21_3",
    "stoch_k_30_3", "stoch_d_30_3",
    "cci_10", "cci_14", "cci_20", "cci_40",
    "atr_7", "atr_14", "atr_21", "atr_30",
    "obv_10", "obv_20", "obv_50",
    "ichimoku_conv_9", "ichimoku_base_26", "ichimoku_spanb_52",
    "ichimoku_conv_7", "ichimoku_base_22",
    "ichimoku_conv_12", "ichimoku_base_33",
    "donchian_high_10", "donchian_low_10",
    "donchian_high_20", "donchian_low_20",
    "donchian_high_50", "donchian_low_50",
    "adx_7", "adx_14", "adx_21", "adx_30",
    "psar_001_02", "psar_002_02", "psar_004_02",
    "close_lag_1", "close_lag_2", "close_lag_3", "close_lag_5", "close_lag_10", "close_lag_20",
    "high_lag_1", "high_lag_2", "high_lag_3", "high_lag_5", "high_lag_10", "high_lag_20",
    "low_lag_1", "low_lag_2", "low_lag_3", "low_lag_5", "low_lag_10", "low_lag_20",
    "open_lag_1", "open_lag_2", "open_lag_3", "open_lag_5", "open_lag_10", "open_lag_20",
    "volume_lag_1", "volume_lag_2", "volume_lag_3", "volume_lag_5", "volume_lag_10", "volume_lag_20"
]

class DatabaseManager:
    """
    Manages database operations for stock prediction ML project
    """
    
    def __init__(self, db_path: str = None, config_path: str = '../config.yaml'):
        """
        Initialize database manager
        
        Parameters:
        -----------
        db_path : str
            Path to SQLite database file
        config_path : str
            Path to configuration file
        """
        if db_path is None:
            db_path = str(Path(__file__).parent / 'stock_database.db')
        
        self.db_path = db_path
        self.config_path = config_path
        self.connection = None
        
        # Load configuration
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}")
            self.config = {}
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row  # Enable column access by name
            self.connection.execute("PRAGMA foreign_keys = ON;")  # Enforce foreign key constraints
            logger.info(f"Connected to database: {self.db_path} (foreign_keys=ON)")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
    
    def execute_script(self, script_path: str):
        """
        Execute SQL script from file
        
        Parameters:
        -----------
        script_path : str
            Path to SQL script file
        """
        if not self.connection:
            self.connect()
        
        try:
            with open(script_path, 'r') as f:
                script = f.read()
            
            self.connection.executescript(script)
            self.connection.commit()
            logger.info(f"Successfully executed script: {script_path}")
        except Exception as e:
            logger.error(f"Failed to execute script {script_path}: {e}")
            self.connection.rollback()
            raise
    
    def setup_database(self):
        """Initialize database schema"""
        schema_path = Path(__file__).parent / 'schema.sql'
        self.execute_script(str(schema_path))
        logger.info("Database schema initialized")
    
    def insert_symbol(self, symbol: str, name: str = None, sector: str = None, industry: str = None, country: str = None, market_cap: str = None, exchange: str = None) -> int:
        """
        Insert symbol if it does not exist, or return its symbol_id. Do NOT overwrite existing fields with None.
        """
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        # Check if symbol exists
        cursor.execute("SELECT symbol_id, name, sector, industry, country, market_cap, exchange FROM symbols WHERE symbol = ?", (symbol,))
        row = cursor.fetchone()
        if row:
            # Symbol exists, return its symbol_id
            return row[0]
        # Insert new symbol with provided fields (may be None)
        cursor.execute("""
            INSERT INTO symbols (symbol, name, sector, industry, country, market_cap, exchange, is_active, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
        """, (symbol, name, sector, industry, country, market_cap, exchange))
        self.connection.commit()
        return cursor.lastrowid
    
    def insert_stock_prices(self, df: pd.DataFrame, symbol: str):
        """
        Insert stock price data
        
        Parameters:
        -----------
        df : pd.DataFrame
            Stock price data with OHLCV columns
        symbol : str
            Stock symbol
        """
        if not self.connection:
            self.connect()
        
        # Get or create symbol ID
        symbol_id = self.insert_symbol(symbol)
        
        try:
            # Prepare data for insertion
            records = []
            for date_idx, row in df.iterrows():
                if isinstance(date_idx, str):
                    date_val = pd.to_datetime(date_idx).date()
                else:
                    date_val = date_idx.date()
                
                records.append((
                    symbol_id,
                    date_val,
                    float(row.get('Open', row.get('open', None))),
                    float(row.get('High', row.get('high', None))),
                    float(row.get('Low', row.get('low', None))),
                    float(row.get('Close', row.get('close', None))),
                    float(row.get('Adj Close', row.get('adj_close', row.get('Close', row.get('close', None))))),
                    int(row.get('Volume', row.get('volume', 0)))
                ))
            
            # Insert records
            cursor = self.connection.cursor()
            cursor.executemany("""
                INSERT OR REPLACE INTO stock_prices 
                (symbol_id, date, open_price, high_price, low_price, close_price, adj_close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, records)
            
            self.connection.commit()
            logger.info(f"Inserted {len(records)} price records for {symbol}")
            
        except Exception as e:
            logger.error(f"Failed to insert stock prices for {symbol}: {e}")
            self.connection.rollback()
            raise
    
    def get_symbols(self) -> pd.DataFrame:
        """Get all symbols from database"""
        if not self.connection:
            self.connect()
        
        return pd.read_sql_query("""
            SELECT symbol_id, symbol, name, sector, market_cap, exchange, is_active
            FROM symbols 
            WHERE is_active = 1
            ORDER BY symbol
        """, self.connection)
    
    def get_stock_prices(self, symbol: str, start_date: date = None, 
                        end_date: date = None) -> pd.DataFrame:
        """
        Get stock price data for symbol
        
        Parameters:
        -----------
        symbol : str
            Stock symbol
        start_date : date
            Start date for data
        end_date : date
            End date for data
            
        Returns:
        --------
        pd.DataFrame
            Stock price data
        """
        if not self.connection:
            self.connect()
        
        query = """
            SELECT sp.date, sp.open_price as open, sp.high_price as high,
                   sp.low_price as low, sp.close_price as close,
                   sp.adj_close, sp.volume
            FROM stock_prices sp
            JOIN symbols s ON sp.symbol_id = s.symbol_id
            WHERE s.symbol = ?
        """
        params = [symbol]
        
        if start_date:
            query += " AND sp.date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND sp.date <= ?"
            params.append(end_date)
        
        query += " ORDER BY sp.date"
        
        df = pd.read_sql_query(query, self.connection, params=params)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        
        return df
    
    def get_recent_stock_prices(self, lookback_days=200):
        query = """
            SELECT * FROM stock_prices
            WHERE date >= DATE('now', ?)
            ORDER BY symbol_id, date
        """
        # '-100 days' for SQLite
        df = pd.read_sql_query(query, self.connection, params=[f'-{lookback_days} days'])
        return df

    def insert_technical_indicators(self, indicators_df, upsert=False, batch_size=100):
        if indicators_df.empty:
            return
        if not self.connection:
            self.connect()
        # Only keep columns that exist in the table
        allowed_cols = [col for col in TECHNICAL_INDICATOR_COLUMNS if col in indicators_df.columns]
        for start in range(0, len(indicators_df), batch_size):
            end = start + batch_size
            batch = indicators_df.iloc[start:end][allowed_cols]
            batch.to_sql(
                'technical_indicators',
                self.connection,
                if_exists='append',
                index=False,
                method='multi'
            )

    def get_all_stock_prices(self) -> pd.DataFrame:
        """
        Get all stock price data, joined with symbol names.
        Returns a DataFrame with columns: symbol, date, open, high, low, close, adj_close, volume
        """
        if not self.connection:
            self.connect()

        query = """
            SELECT s.symbol, s.symbol_id, sp.date, sp.open_price as open, sp.high_price as high,
                   sp.low_price as low, sp.close_price as close, sp.adj_close, sp.volume
            FROM stock_prices sp
            JOIN symbols s ON sp.symbol_id = s.symbol_id
            ORDER BY s.symbol, sp.date
        """
        df = pd.read_sql_query(query, self.connection)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df

    def get_all_technical_indicators(self) -> pd.DataFrame:
        """
        Get all technical indicator data.
        Returns a DataFrame with columns: symbol, date, ...[all indicator columns]...
        """
        if not self.connection:
            self.connect()

        query = """
            SELECT ti.*, s.symbol
            FROM technical_indicators ti
            JOIN symbols s ON ti.symbol_id = s.symbol_id
            ORDER BY s.symbol, ti.date
        """
        df = pd.read_sql_query(query, self.connection)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df

    def insert_technical_trade_signals(self, signals_df, batch_size=100):
        """
        Insert trade signals into the technical_trade_signals table.
        Dynamically filters columns to match the table schema.
        Performs upsert by deleting existing rows for (symbol_id, date) before insert.
        """
        if signals_df.empty:
            return
        if not self.connection:
            self.connect()
        # Drop 'symbol' column if present
        if 'symbol' in signals_df.columns:
            signals_df = signals_df.drop(columns=['symbol'])
        # Dynamically get columns from the table schema
        cursor = self.connection.cursor()
        cursor.execute("PRAGMA table_info(technical_trade_signals)")
        table_info = cursor.fetchall()
        table_columns = [col[1] for col in table_info]
        # Only keep columns that exist in the table
        allowed_cols = [col for col in signals_df.columns if col in table_columns]
        dropped_cols = [col for col in signals_df.columns if col not in table_columns]
        if dropped_cols:
            print(f"[insert_technical_trade_signals] Dropping columns not in schema: {dropped_cols}")
        for start in range(0, len(signals_df), batch_size):
            end = start + batch_size
            batch = signals_df.iloc[start:end][allowed_cols].copy()
            # Ensure date is string for SQLite
            if 'date' in batch.columns:
                batch['date'] = batch['date'].astype(str)
            # Upsert: delete existing rows for (symbol_id, date) in this batch
            if not batch.empty:
                symbol_date_tuples = list(batch[['symbol_id', 'date']].itertuples(index=False, name=None))
                cursor.executemany(
                    "DELETE FROM technical_trade_signals WHERE symbol_id = ? AND date = ?",
                    symbol_date_tuples
                )
                self.connection.commit()
            batch.to_sql(
                'technical_trade_signals',
                self.connection,
                if_exists='append',
                index=False,
                method='multi'
            )

    def insert_outcomes(self, outcomes_df, batch_size=100):
        """
        Insert outcomes into the outcomes table.
        """
        if outcomes_df.empty:
            return
        if not self.connection:
            self.connect()
        for start in range(0, len(outcomes_df), batch_size):
            end = start + batch_size
            batch = outcomes_df.iloc[start:end]
            batch.to_sql(
                'outcomes',
                self.connection,
                if_exists='append',
                index=False,
                method='multi'
            )

    def insert_calendar(self, calendar_df, batch_size=100):
        """
        Insert calendar features into the calendar table.
        """
        if calendar_df.empty:
            return
        if not self.connection:
            self.connect()
        # Ensure date is string for SQLite
        if 'date' in calendar_df.columns:
            calendar_df = calendar_df.copy()
            calendar_df['date'] = pd.to_datetime(calendar_df['date']).dt.strftime('%Y-%m-%d')
        for start in range(0, len(calendar_df), batch_size):
            end = start + batch_size
            batch = calendar_df.iloc[start:end]
            batch.to_sql(
                'calendar',
                self.connection,
                if_exists='append',
                index=False,
                method='multi'
            )