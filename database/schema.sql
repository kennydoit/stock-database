-- Stock Prediction ML Database Schema
-- Tables for storing stock data, news data, and features

-- Table for storing stock symbols and metadata
CREATE TABLE IF NOT EXISTS symbols (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    name VARCHAR(255),
    sector VARCHAR(100),
    market_cap VARCHAR(20),
    exchange VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for storing daily stock price data
CREATE TABLE IF NOT EXISTS stock_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(10,4),
    high_price DECIMAL(10,4),
    low_price DECIMAL(10,4),
    close_price DECIMAL(10,4),
    adj_close DECIMAL(10,4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id),
    UNIQUE(symbol_id, date)
);

-- Table for storing news articles
--CREATE TABLE IF NOT EXISTS news_articles (
--    id INTEGER PRIMARY KEY AUTOINCREMENT,
--    headline TEXT NOT NULL,
--    summary TEXT,
--    content TEXT,
--    author VARCHAR(255),
--    source VARCHAR(100),
--    url TEXT,
--    published_at TIMESTAMP NOT NULL,
--    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--    sentiment_score DECIMAL(3,2),
--    sentiment_label VARCHAR(20)
--);

-- Table for linking news articles to symbols (many-to-many)
--CREATE TABLE IF NOT EXISTS news_symbols (
--    id INTEGER PRIMARY KEY AUTOINCREMENT,
--    news_id INTEGER NOT NULL,
--    symbol_id INTEGER NOT NULL,
--    relevance_score DECIMAL(3,2),
--    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--    FOREIGN KEY (news_id) REFERENCES news_articles(id),
--    FOREIGN KEY (symbol_id) REFERENCES symbols(id),
--    UNIQUE(news_id, symbol_id)
--);

-- Table for storing computed features
CREATE TABLE IF NOT EXISTS features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    date DATE NOT NULL,
    feature_name VARCHAR(100) NOT NULL,
    feature_value DECIMAL(15,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id),
    UNIQUE(symbol_id, date, feature_name)
);

-- Table for storing model predictions
CREATE TABLE IF NOT EXISTS predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    prediction_date DATE NOT NULL,
    target_date DATE NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    prediction_value DECIMAL(10,4),
    confidence_score DECIMAL(3,2),
    actual_value DECIMAL(10,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id)
);

-- Add model tracking table
CREATE TABLE IF NOT EXISTS model_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT UNIQUE NOT NULL,
    target_symbol TEXT NOT NULL,
    model_type TEXT NOT NULL,
    train_r2 REAL,
    test_r2 REAL,
    train_mse REAL,
    test_mse REAL,
    n_features INTEGER,
    feature_selection_method TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    config_snapshot TEXT,  -- JSON string of config
    notes TEXT
);

-- Add feature importance tracking
CREATE TABLE IF NOT EXISTS feature_importance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    coefficient REAL,
    abs_coefficient REAL,
    p_value REAL,
    rank_importance INTEGER,
    FOREIGN KEY (run_id) REFERENCES model_runs (run_id)
);

-- Add prediction tracking
CREATE TABLE IF NOT EXISTS model_predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    prediction_date DATE NOT NULL,
    actual_return REAL,
    predicted_return REAL,
    residual REAL,
    data_type TEXT CHECK (data_type IN ('train', 'test', 'validation')),
    FOREIGN KEY (run_id) REFERENCES model_runs (run_id)
);

-- Table for storing technical indicators
CREATE TABLE IF NOT EXISTS technical_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    date DATE NOT NULL,

    -- RSI
    rsi_7 REAL, rsi_14 REAL, rsi_30 REAL, rsi_50 REAL,

    -- SMA
    sma_5 REAL, sma_10 REAL, sma_20 REAL, sma_50 REAL, sma_100 REAL, sma_200 REAL,

    -- EMA
    ema_5 REAL, ema_10 REAL, ema_20 REAL, ema_50 REAL, ema_100 REAL, ema_200 REAL,

    -- MACD (macd_{fast}_{slow}_{signal})
    macd_6_13_5 REAL, macd_signal_6_13_5 REAL, macd_hist_6_13_5 REAL,
    macd_12_26_9 REAL, macd_signal_12_26_9 REAL, macd_hist_12_26_9 REAL,
    macd_19_39_9 REAL, macd_signal_19_39_9 REAL, macd_hist_19_39_9 REAL,

    -- Bollinger Bands (bb_upper_{window}, etc.)
    bb_upper_10 REAL, bb_middle_10 REAL, bb_lower_10 REAL,
    bb_upper_14 REAL, bb_middle_14 REAL, bb_lower_14 REAL,
    bb_upper_20 REAL, bb_middle_20 REAL, bb_lower_20 REAL,
    bb_upper_50 REAL, bb_middle_50 REAL, bb_lower_50 REAL,

    -- Stochastic Oscillator
    stoch_k_7_3 REAL, stoch_d_7_3 REAL,
    stoch_k_10_3 REAL, stoch_d_10_3 REAL,
    stoch_k_14_3 REAL, stoch_d_14_3 REAL,
    stoch_k_21_3 REAL, stoch_d_21_3 REAL,
    stoch_k_30_3 REAL, stoch_d_30_3 REAL,

    -- CCI
    cci_10 REAL, cci_14 REAL, cci_20 REAL, cci_40 REAL,

    -- ATR
    atr_7 REAL, atr_14 REAL, atr_21 REAL, atr_30 REAL,

    -- OBV (rolling windows)
    obv_10 REAL, obv_20 REAL, obv_50 REAL,

    -- Ichimoku
    ichimoku_conv_9 REAL, ichimoku_base_26 REAL, ichimoku_spanb_52 REAL,
    ichimoku_conv_7 REAL, ichimoku_base_22 REAL,
    ichimoku_conv_12 REAL, ichimoku_base_33 REAL,

    -- Donchian Channel
    donchian_high_10 REAL, donchian_low_10 REAL,
    donchian_high_20 REAL, donchian_low_20 REAL,
    donchian_high_50 REAL, donchian_low_50 REAL,

    -- ADX
    adx_7 REAL, adx_14 REAL, adx_21 REAL, adx_30 REAL,

    -- Parabolic SAR
    psar_001_02 REAL, psar_002_02 REAL, psar_004_02 REAL,

    -- Lags (example for Close, repeat for High, Low, Open, Volume)
    close_lag_1 REAL, close_lag_2 REAL, close_lag_3 REAL, close_lag_5 REAL, close_lag_10 REAL, close_lag_20 REAL,
    high_lag_1 REAL, high_lag_2 REAL, high_lag_3 REAL, high_lag_5 REAL, high_lag_10 REAL, high_lag_20 REAL,
    low_lag_1 REAL, low_lag_2 REAL, low_lag_3 REAL, low_lag_5 REAL, low_lag_10 REAL, low_lag_20 REAL,
    open_lag_1 REAL, open_lag_2 REAL, open_lag_3 REAL, open_lag_5 REAL, open_lag_10 REAL, open_lag_20 REAL,
    volume_lag_1 REAL, volume_lag_2 REAL, volume_lag_3 REAL, volume_lag_5 REAL, volume_lag_10 REAL, volume_lag_20 REAL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id),
    UNIQUE(symbol_id, date)
);

-- Table for storing technical trade signals
CREATE TABLE IF NOT EXISTS technical_trade_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    date DATE NOT NULL,

    -- Example signals (expand as needed)
    rsi_signal_7 INTEGER, rsi_signal_14 INTEGER, rsi_signal_30 INTEGER, rsi_signal_50 INTEGER,
    macd_signal_12_26_9 INTEGER, macd_signal_6_13_5 INTEGER, macd_signal_19_39_9 INTEGER,
    bb_signal_10 INTEGER, bb_signal_14 INTEGER, bb_signal_20 INTEGER, bb_signal_50 INTEGER,
    stoch_signal_7_3 INTEGER, stoch_signal_14_3 INTEGER, stoch_signal_30_3 INTEGER,
    cci_signal_10 INTEGER, cci_signal_20 INTEGER, cci_signal_40 INTEGER,
    adx_signal_7 INTEGER, adx_signal_14 INTEGER, adx_signal_21 INTEGER, adx_signal_30 INTEGER,
    donchian_signal_10 INTEGER, donchian_signal_20 INTEGER, donchian_signal_50 INTEGER,
    psar_signal_001_02 INTEGER, psar_signal_002_02 INTEGER, psar_signal_004_02 INTEGER,

    -- Add more as needed...

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id),
    UNIQUE(symbol_id, date)
);

-- Table for storing predicted vs actual outcomes
CREATE TABLE IF NOT EXISTS outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    date DATE NOT NULL,
    price_d1 REAL,
    price_d3 REAL,
    price_d5 REAL,
    price_d7 REAL,
    price_d10 REAL,
    price_d14 REAL,
    price_d21 REAL,
    price_d28 REAL,
    price_d60 REAL,
    price_d90 REAL,
    price_d120 REAL,
    returns_d1 REAL,
    returns_d3 REAL,
    returns_d5 REAL,
    returns_d7 REAL,
    returns_d10 REAL,
    returns_d14 REAL,
    returns_d21 REAL,
    returns_d28 REAL,
    returns_d60 REAL,
    returns_d90 REAL,
    returns_d120 REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id),
    UNIQUE(symbol_id, date)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date ON stock_prices(symbol_id, date);
--CREATE INDEX IF NOT EXISTS idx_news_published_at ON news_articles(published_at);
--CREATE INDEX IF NOT EXISTS idx_news_symbols_symbol ON news_symbols(symbol_id);
CREATE INDEX IF NOT EXISTS idx_features_symbol_date ON features(symbol_id, date);
CREATE INDEX IF NOT EXISTS idx_predictions_symbol_date ON predictions(symbol_id, prediction_date);
CREATE INDEX IF NOT EXISTS idx_model_runs_symbol ON model_runs(target_symbol);
CREATE INDEX IF NOT EXISTS idx_model_runs_created ON model_runs(created_at);
CREATE INDEX IF NOT EXISTS idx_feature_importance_run ON feature_importance(run_id);
CREATE INDEX IF NOT EXISTS idx_predictions_run_date ON model_predictions(run_id, prediction_date);
CREATE INDEX IF NOT EXISTS idx_technical_indicators_symbol_date ON technical_indicators(symbol_id, date);
CREATE INDEX IF NOT EXISTS idx_technical_trade_signals_symbol_date ON technical_trade_signals(symbol_id, date);
CREATE INDEX IF NOT EXISTS idx_outcomes_symbol_date ON outcomes(symbol_id, date);