import yfinance as yf
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, or_
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

engine = create_engine(DATABASE_URL, connect_args={"options": "-c statement_timeout=60000"})
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '500'))
SYMBOLS_PER_RUN = int(os.environ.get('SYMBOLS_PER_RUN', '2000'))
QUARANTINE_DAYS = int(os.environ.get('QUARANTINE_DAYS', '7'))

def get_active_symbols():
    """Get symbols that are not quarantined"""
    now = datetime.now(timezone.utc)
    
    query = session.query(StockSymbol).filter(
        or_(
            StockSymbol.quarantined_until == None,
            StockSymbol.quarantined_until < now
        )
    )
    
    return query

def get_rotation_offset():
    """Calculate offset based on time to rotate through all symbols"""
    current_hour = datetime.now(timezone.utc).hour
    current_minute = datetime.now(timezone.utc).minute
    
    # Each 15-minute slot gets a different batch
    slot = (current_hour * 4) + (current_minute // 15)
    return (slot * SYMBOLS_PER_RUN) % 50000  # Wrap around

def quarantine_symbols(symbols):
    """Put failed symbols in quarantine for QUARANTINE_DAYS"""
    if not symbols:
        return
    
    quarantine_until = datetime.now(timezone.utc) + timedelta(days=QUARANTINE_DAYS)
    
    try:
        count = 0
        for symbol in symbols[:100]:  # Limit to avoid long queries
            result = session.query(StockSymbol).filter(
                StockSymbol.symbol == symbol
            ).update({StockSymbol.quarantined_until: quarantine_until})
            count += result
        session.commit()
        print(f"Quarantined {count} symbols for {QUARANTINE_DAYS} days")
    except Exception as e:
        session.rollback()
        print(f"Error quarantining symbols: {e}")

def fetch_stock_prices():
    """Fetch stock prices with rotation and quarantine support"""
    print("Fetching stock prices...")
    
    # Get active (non-quarantined) symbols
    active_query = get_active_symbols()
    total_active = active_query.count()
    
    # Get rotation offset
    offset = get_rotation_offset() % max(total_active, 1)
    
    # Get batch of symbols for this run
    stock_symbols = active_query.offset(offset).limit(SYMBOLS_PER_RUN).all()
    
    # If we got less than expected, wrap around from beginning
    if len(stock_symbols) < SYMBOLS_PER_RUN and offset > 0:
        remaining = SYMBOLS_PER_RUN - len(stock_symbols)
        more_symbols = active_query.limit(remaining).all()
        stock_symbols.extend(more_symbols)
    
    if not stock_symbols:
        print("No active stock symbols found.")
        return [], []
    
    all_symbols = [s.symbol for s in stock_symbols]
    symbol_names = {s.symbol: s.name for s in stock_symbols}
    
    print(f"Active symbols: {total_active}, Fetching: {len(all_symbols)} (offset: {offset})")
    
    stock_data = []
    failed_symbols = []
    
    # Process in batches
    for i in range(0, len(all_symbols), BATCH_SIZE):
        batch_symbols = all_symbols[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(all_symbols) + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"Batch {batch_num}/{total_batches} ({len(batch_symbols)} symbols)...")
        
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
            
            if i + BATCH_SIZE < len(all_symbols):
                time.sleep(0.5)
                
        except Exception as e:
            print(f"Batch {batch_num} failed: {e}")
            failed_symbols.extend(batch_symbols)
            time.sleep(1)
            continue
    
    print(f"Fetched {len(stock_data)} prices, {len(failed_symbols)} failed")
    
    return stock_data, failed_symbols

def save_stock_prices(stock_data, batch_size=50):
    """Save stock prices to database"""
    global session
    
    if not stock_data:
        return
    
    saved_count = 0
    for i in range(0, len(stock_data), batch_size):
        batch = stock_data[i:i + batch_size]
        try:
            session.bulk_save_objects([StockPrice(**entry) for entry in batch])
            session.commit()
            saved_count += len(batch)
        except Exception as e:
            session.rollback()
            print(f"Error saving batch: {e}")
            try:
                session.close()
                session = Session()
            except:
                pass
    
    print(f"Saved {saved_count}/{len(stock_data)} prices")
    
    # Clean old records
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
    try:
        deleted = session.query(StockPrice).filter(
            StockPrice.timestamp < cutoff_time
        ).delete(synchronize_session=False)
        session.commit()
        if deleted:
            print(f"Cleaned up {deleted} old records")
    except Exception as e:
        session.rollback()

if __name__ == "__main__":
    stock_data, failed_symbols = fetch_stock_prices()
    save_stock_prices(stock_data)
    
    # Quarantine failed symbols
    if failed_symbols:
        quarantine_symbols(failed_symbols)
    
    # Check for price alerts
    try:
        from alerts import check_price_alerts
        check_price_alerts(session, StockPrice)
    except Exception as e:
        print(f"Alert check failed: {e}")
