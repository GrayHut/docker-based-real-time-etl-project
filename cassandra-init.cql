CREATE KEYSPACE binance WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE binance;
CREATE TABLE latest_prices (
    id INT PRIMARY KEY,
    symbol TEXT,
    price DECIMAL,
    timestamp TIMESTAMP
);
CREATE TABLE order_book (
    id INT PRIMARY KEY,
    symbol TEXT,
    bid_price DECIMAL,
    bid_quantity DECIMAL,
    ask_price DECIMAL,
    ask_quantity DECIMAL,
    timestamp TIMESTAMP
);
CREATE TABLE recent_trades (
    id INT PRIMARY KEY,
    symbol TEXT,
    price DECIMAL,
    quantity DECIMAL,
    trade_time BIGINT,
    is_buyer_maker BOOLEAN,
    timestamp TIMESTAMP
);
CREATE TABLE klines (
    id INT PRIMARY KEY,
    symbol TEXT,
    open_time BIGINT,
    open_price DECIMAL,
    high_price DECIMAL,
    low_price DECIMAL,
    close_price DECIMAL,
    volume DECIMAL,
    close_time BIGINT,
    timestamp TIMESTAMP
);
CREATE TABLE ticker_24hr (
    id INT PRIMARY KEY,
    symbol TEXT,
    price_change DECIMAL,
    price_change_percent DECIMAL,
    weighted_avg_price DECIMAL,
    prev_close_price DECIMAL,
    last_price DECIMAL,
    volume DECIMAL,
    timestamp TIMESTAMP
);