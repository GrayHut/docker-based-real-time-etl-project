import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Binance API configuration
BASE_URL = "https://api.binance.com/api/v3"
ENDPOINTS = {
    "latest_prices": "/ticker/price",
    "order_book": "/depth",
    "recent_trades": "/trades",
    "klines": "/klines",
    "ticker_24hr": "/ticker/24hr"
}

# PostgreSQL configuration
DB_CONFIG = {
    'dbname': 'binance_crypto_db',
    'user': 'postgres',
    'password': '1234',
    'host': 'postgres',
    'port': '5432'
}

def create_tables(engine):
    """Create necessary tables in PostgreSQL if they don't exist"""
    create_tables_sql = [
        """
        CREATE TABLE IF NOT EXISTS latest_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50),
            price DECIMAL(20,8),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS order_book (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50),
            bid_price DECIMAL(20,8),
            bid_quantity DECIMAL(20,8),
            ask_price DECIMAL(20,8),
            ask_quantity DECIMAL(20,8),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS recent_trades (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50),
            price DECIMAL(20,8),
            quantity DECIMAL(20,8),
            trade_time BIGINT,
            is_buyer_maker BOOLEAN,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
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
        )
        """,
        """
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
        )
        """
    ]
    
    try:
        with engine.connect() as conn:
            for sql in create_tables_sql:
                conn.execute(text(sql))
            conn.commit()
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")

def fetch_binance_data(endpoint, symbol="BTCUSDT", **params):
    """Fetch data from Binance API"""
    url = f"{BASE_URL}{ENDPOINTS[endpoint]}"
    params['symbol'] = symbol
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching {endpoint} data: {e}")
        return None

def transform_latest_prices(data):
    """Transform latest prices data into DataFrame"""
    if isinstance(data, list):
        df = pd.DataFrame(data)[['symbol', 'price']]
    else:
        df = pd.DataFrame([data])[['symbol', 'price']]
        
    df['price'] = df['price'].astype(float)
    return df

def transform_order_book(data, symbol):
    """Transform order book data into DataFrame"""
    bids = data.get('bids', [])[:5]
    asks = data.get('asks', [])[:5]
    df = pd.DataFrame({
        'symbol': symbol,
        'bid_price': [float(bid[0]) for bid in bids],
        'bid_quantity': [float(bid[1]) for bid in bids],
        'ask_price': [float(ask[0]) for ask in asks],
        'ask_quantity': [float(ask[1]) for ask in asks]
    })
    return df

def transform_recent_trades(data, symbol):
    """Transform recent trades data into DataFrame"""
    df = pd.DataFrame(data[:50])[['price', 'qty', 'time', 'isBuyerMaker']]
    df['symbol'] = symbol
    df = df.rename(columns={'qty': 'quantity', 'time': 'trade_time', 'isBuyerMaker': 'is_buyer_maker'})
    df['price'] = df['price'].astype(float)
    df['quantity'] = df['quantity'].astype(float)
    return df

def transform_klines(data, symbol):
    """Transform klines data into DataFrame"""
    df = pd.DataFrame(data, columns=[
        'open_time', 'open_price', 'high_price', 'low_price', 'close_price',
        'volume', 'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])
    df['symbol'] = symbol
    df = df[['symbol', 'open_time', 'open_price', 'high_price', 'low_price', 
             'close_price', 'volume', 'close_time']]
    df[['open_price', 'high_price', 'low_price', 'close_price', 'volume']] = \
        df[['open_price', 'high_price', 'low_price', 'close_price', 'volume']].astype(float)
    return df

def transform_ticker_24hr(data):
    """Transform 24hr ticker data into DataFrame"""
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame([data])
    df = df[['symbol', 'priceChange', 'priceChangePercent', 'weightedAvgPrice', 
             'prevClosePrice', 'lastPrice', 'volume']]
    df = df.rename(columns={
        'priceChange': 'price_change',
        'priceChangePercent': 'price_change_percent',
        'weightedAvgPrice': 'weighted_avg_price',
        'prevClosePrice': 'prev_close_price',
        'lastPrice': 'last_price'
    })
    df[['price_change', 'price_change_percent', 'weighted_avg_price', 
        'prev_close_price', 'last_price', 'volume']] = \
        df[['price_change', 'price_change_percent', 'weighted_avg_price', 
            'prev_close_price', 'last_price', 'volume']].astype(float)
    return df

def insert_data(engine, table, df):
    """Insert DataFrame into PostgreSQL"""
    try:
        df.to_sql(table, engine, if_exists='append', index=False)
        logger.info(f"Successfully inserted {len(df)} records into {table}")
    except Exception as e:
        logger.error(f"Error inserting into {table}: {e}")

def main():
    # Create SQLAlchemy engine
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
            #f",pool_use_lifo=True, pool_pre_ping=True"
        )
        logger.info("Connected to PostgreSQL database")
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return

    # Create tables
    # create_tables(engine)

    symbol = "BTCUSDT"
    
    while True:
        try:
            # Fetch and process latest prices
            latest_prices = fetch_binance_data("latest_prices", symbol)
            if latest_prices:
                df_prices = transform_latest_prices(latest_prices)
                insert_data(engine, "latest_prices", df_prices)

            # Fetch and process order book
            order_book = fetch_binance_data("order_book", symbol, limit=5)
            if order_book:
                df_book = transform_order_book(order_book, symbol)
                insert_data(engine, "order_book", df_book)

            # Fetch and process recent trades
            recent_trades = fetch_binance_data("recent_trades", symbol, limit=50)
            if recent_trades:
                df_trades = transform_recent_trades(recent_trades, symbol)
                insert_data(engine, "recent_trades", df_trades)

            # Fetch and process klines (1 hour interval)
            klines = fetch_binance_data("klines", symbol, interval='1h', limit=100)
            if klines:
                df_klines = transform_klines(klines, symbol)
                insert_data(engine, "klines", df_klines)

            # Fetch and process 24hr ticker
            ticker_24hr = fetch_binance_data("ticker_24hr", symbol)
            if ticker_24hr:
                df_ticker = transform_ticker_24hr(ticker_24hr)
                insert_data(engine, "ticker_24hr", df_ticker)

            # Sleep to respect API rate limits
            time.sleep(60)  # Wait 1 minute before next fetch

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying on error

if __name__ == "__main__":
    main()