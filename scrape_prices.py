import yfinance as yf
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
import os
from datetime import datetime, timezone, timedelta
from models import StockSymbol
import time

Base = declarative_base()

class StockPrice(Base):
    __tablename__ = 'stock_prices'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False)
    name = Column(String, nullable=True)
    price = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL environment variable set")

engine = create_engine(DATABASE_URL, connect_args={"options": "-c statement_timeout=30000"})  # 30 second timeout
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

YAHOO_URL = "https://finance.yahoo.com/quote"

def fetch_stock_prices():
    print("Trying to fetch stock prices")
    stock_symbols = session.query(StockSymbol).all()
    if not stock_symbols:
        print("No stock symbols found in the database.")
        return []
    stock_data = []
    for stock_symbol in stock_symbols:
        symbol = stock_symbol.symbol
        name = stock_symbol.name
        try:
            print(symbol)
            stock = yf.Ticker(symbol)
            print(stock)
            price_data = stock.info.get("currentPrice")
            if price_data is not None:
                stock_data.append({
                    "symbol": symbol,
                    "name": name,
                    "price": price_data
                })
            else:
                print(f"No price data for symbol: {symbol}")
        except Exception as e:
            print(f"Error fetching data for symbol: {symbol}, Error: {e}")
    return stock_data

def save_stock_prices(stock_data, batch_size=20):
    for i in range(0, len(stock_data), batch_size):
        batch = stock_data[i:i + batch_size]
        try:
            session.bulk_save_objects([StockPrice(**entry) for entry in batch])
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error saving batch: {e}")
            time.sleep(5)  
            try:
                session.bulk_save_objects([StockPrice(**entry) for entry in batch])
                session.commit()
            except Exception as e:
                session.rollback()
                print(f"Error saving batch after retry: {e}")

    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
    try:
        session.query(StockPrice).filter(StockPrice.timestamp < cutoff_time).delete(synchronize_session=False)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error deleting old records: {e}")

if __name__ == "__main__":
    stock_data = fetch_stock_prices()
    save_stock_prices(stock_data)
