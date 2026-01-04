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

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://")

if "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require" if "?" not in DATABASE_URL else "&sslmode=require"

engine = create_engine(DATABASE_URL, connect_args={"options": "-c statement_timeout=30000"})
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

YAHOO_URL = "https://finance.yahoo.com/quote"

BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '200'))

MAX_SYMBOLS = int(os.environ.get('MAX_SYMBOLS', '0'))

def fetch_stock_prices():
    """Fetch ALL stock prices using batch downloading with fast_info"""
    print("Fetching stock prices (batch mode - ALL symbols)...")
    
    if MAX_SYMBOLS > 0:
        stock_symbols = session.query(StockSymbol).limit(MAX_SYMBOLS).all()
    else:
        stock_symbols = session.query(StockSymbol).all()
    
    if not stock_symbols:
        print("No stock symbols found in the database.")
        return []
    
    all_symbols = [s.symbol for s in stock_symbols]
    symbol_names = {s.symbol: s.name for s in stock_symbols}
    
    print(f"Total symbols to fetch: {len(all_symbols)}")
    
    stock_data = []
    failed_symbols = []
    
    for i in range(0, len(all_symbols), BATCH_SIZE):
        batch_symbols = all_symbols[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(all_symbols) + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"Processing batch {batch_num}/{total_batches} ({len(batch_symbols)} symbols)...")
        
        try:
            tickers = yf.Tickers(" ".join(batch_symbols))
            
            for symbol in batch_symbols:
                try:
                    ticker = tickers.tickers.get(symbol)
                    if ticker is None:
                        failed_symbols.append(symbol)
                        continue
                    
                    info = ticker.fast_info
                    price = None
                    
                    if hasattr(info, 'last_price') and info.last_price:
                        price = info.last_price
                    elif hasattr(info, 'previous_close') and info.previous_close:
                        price = info.previous_close
                    
                    if price is not None and price > 0:
                        stock_data.append({
                            "symbol": symbol,
                            "name": symbol_names.get(symbol),
                            "price": float(price)
                        })
                    else:
                        failed_symbols.append(symbol)
                        
                except Exception as e:
                    failed_symbols.append(symbol)
                    continue
            
            # Small delay between batches to be nice to Yahoo
            if i + BATCH_SIZE < len(all_symbols):
                time.sleep(1)
                
        except Exception as e:
            print(f"Batch {batch_num} failed: {e}")
            failed_symbols.extend(batch_symbols)
            time.sleep(2)  
            continue
    
    print(f"Successfully fetched {len(stock_data)} stock prices")
    if failed_symbols:
        print(f"Failed to fetch {len(failed_symbols)} symbols")
    
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
    
    # Check for price alerts and send notifications
    try:
        from alerts import check_price_alerts
        check_price_alerts(session, StockPrice)
    except Exception as e:
        print(f"Alert check failed: {e}")
