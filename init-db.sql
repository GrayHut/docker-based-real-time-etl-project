CREATE DATABASE binance_crypto_db;
\c binance_crypto_db
CREATE TABLE IF NOT EXISTS latest_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50),
    price DECIMAL(20,8),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS order_book (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50),
    bid_price DECIMAL(20,8),
    bid_quantity DECIMAL(20,8),
    ask_price DECIMAL(20,8),
    ask_quantity DECIMAL(20,8),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS recent_trades (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50),
    price DECIMAL(20,8),
    quantity DECIMAL(20,8),
    trade_time BIGINT,
    is_buyer_maker BOOLEAN,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS klines (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50),
    open_time BIGINT,
    open_price DECIMAL(20,8),
    high_price DECIMAL(20,8),
    low_price DECIMAL(20,8),
    close_price DECIMAL(20,8),
    volume DECIMAL(20,8),
    close_time BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ticker_24hr (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50),
    price_change DECIMAL(20,8),
    price_change_percent DECIMAL(10,4),
    weighted_avg_price DECIMAL(20,8),
    prev_close_price DECIMAL(20,8),
    last_price DECIMAL(20,8),
    volume DECIMAL(20,8),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'debezium_pass';
-- GRANT ALL PRIVILEGES ON DATABASE binance_crypto_db TO debezium;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
-- SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

GRANT ALL PRIVILEGES ON DATABASE binance_crypto_db TO postgres;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO postgres;
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');