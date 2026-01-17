import yfinance as yf
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, BigInteger, or_
from sqlalchemy.orm import declarative_base, sessionmaker
import os
from datetime import datetime, timezone, timedelta
from models import StockSymbol
import time
import signal
import sys

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

Base = declarative_base()

class StockPrice(Base):
    __tablename__ = 'stock_prices'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False)
    name = Column(String, nullable=True)
    price = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL environment variable set")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://")

if "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require" if "?" not in DATABASE_URL else "&sslmode=require"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '100'))
SYMBOLS_PER_RUN = int(os.environ.get('SYMBOLS_PER_RUN', '300'))
QUARANTINE_DAYS = int(os.environ.get('QUARANTINE_DAYS', '7'))
MAX_RUNTIME_SECONDS = int(os.environ.get('MAX_RUNTIME_SECONDS', '480'))

start_time = time.time()
stock_data_global = []
failed_symbols_global = []

def time_remaining():
    elapsed = time.time() - start_time
    return MAX_RUNTIME_SECONDS - elapsed

def should_stop():
    return time_remaining() < 60

log(f"Config: BATCH={BATCH_SIZE}, SYMBOLS={SYMBOLS_PER_RUN}, MAX_TIME={MAX_RUNTIME_SECONDS}s")

def get_active_symbols():
    now = datetime.now(timezone.utc)
    return session.query(StockSymbol).filter(
        or_(
            StockSymbol.quarantined_until == None,
            StockSymbol.quarantined_until < now
        )
    )

def get_rotation_offset():
    current_hour = datetime.now(timezone.utc).hour
    current_minute = datetime.now(timezone.utc).minute
    slot = (current_hour * 4) + (current_minute // 15)
    return (slot * SYMBOLS_PER_RUN) % 50000

def quarantine_symbols(symbols):
    if not symbols:
        return
    
    quarantine_until = datetime.now(timezone.utc) + timedelta(days=QUARANTINE_DAYS)
    
    try:
        count = 0
        for symbol in symbols[:50]:
            result = session.query(StockSymbol).filter(
                StockSymbol.symbol == symbol
            ).update({StockSymbol.quarantined_until: quarantine_until})
            count += result
        session.commit()
        log(f"Quarantined {count} symbols")
    except Exception as e:
        session.rollback()

def fetch_batch_fast(symbols, symbol_names):
    if not symbols:
        return [], []
    
    stock_data = []
    failed = []
    
    try:
        data = yf.download(
            symbols,
            period="1d",
            interval="1d",
            progress=False,
            threads=True,
            timeout=30
        )
        
        if data.empty:
            return [], symbols
        
        for symbol in symbols:
            try:
                if len(symbols) == 1:
                    close = data['Close'].iloc[-1] if 'Close' in data else None
                    vol = data['Volume'].iloc[-1] if 'Volume' in data else None
                else:
                    close = data['Close'][symbol].iloc[-1] if symbol in data['Close'].columns else None
                    vol = data['Volume'][symbol].iloc[-1] if symbol in data['Volume'].columns else None
                
                if close and close > 0 and not (hasattr(close, 'isna') and close.isna()):
                    stock_data.append({
                        "symbol": symbol,
                        "name": symbol_names.get(symbol),
                        "price": float(close),
                        "volume": int(vol) if vol and not (hasattr(vol, 'isna') and vol.isna()) else None
                    })
                else:
                    failed.append(symbol)
            except:
                failed.append(symbol)
    except Exception as e:
        log(f"Download error: {e}")
        failed = symbols
    
    return stock_data, failed

def fetch_stock_prices():
    global stock_data_global, failed_symbols_global
    
    log("Fetching stock prices...")
    
    active_query = get_active_symbols()
    total_active = active_query.count()
    log(f"Active symbols: {total_active}")
    
    offset = get_rotation_offset() % max(total_active, 1)
    stock_symbols = active_query.offset(offset).limit(SYMBOLS_PER_RUN).all()
    
    if not stock_symbols:
        log("No symbols found")
        return [], []
    
    all_symbols = [s.symbol for s in stock_symbols]
    symbol_names = {s.symbol: s.name for s in stock_symbols}
    
    log(f"Fetching {len(all_symbols)} symbols (offset: {offset})")
    
    stock_data = []
    failed_symbols = []
    
    for i in range(0, len(all_symbols), BATCH_SIZE):
        if should_stop():
            log(f"Time limit approaching, stopping early. Got {len(stock_data)} prices.")
            break
        
        batch_start = time.time()
        batch_symbols = all_symbols[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(all_symbols) + BATCH_SIZE - 1) // BATCH_SIZE
        
        log(f"Batch {batch_num}/{total_batches} ({len(batch_symbols)} symbols)...")
        
        data, failed = fetch_batch_fast(batch_symbols, symbol_names)
        stock_data.extend(data)
        failed_symbols.extend(failed)
        
        batch_time = time.time() - batch_start
        log(f"Batch {batch_num} done in {batch_time:.1f}s - {len(data)} prices, {len(failed)} failed")
        
        time.sleep(0.2)
    
    stock_data_global = stock_data
    failed_symbols_global = failed_symbols
    
    log(f"Total: {len(stock_data)} prices, {len(failed_symbols)} failed")
    return stock_data, failed_symbols

def save_stock_prices(stock_data, batch_size=50):
    global session
    
    if not stock_data:
        return
    
    log(f"Saving {len(stock_data)} prices...")
    saved_count = 0
    
    for i in range(0, len(stock_data), batch_size):
        batch = stock_data[i:i + batch_size]
        try:
            session.bulk_save_objects([StockPrice(**entry) for entry in batch])
            session.commit()
            saved_count += len(batch)
        except Exception as e:
            session.rollback()
            try:
                session.close()
                session = Session()
            except:
                pass
    
    log(f"Saved {saved_count} prices")
    
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
    try:
        deleted = session.query(StockPrice).filter(
            StockPrice.timestamp < cutoff_time
        ).delete(synchronize_session=False)
        session.commit()
        if deleted:
            log(f"Cleaned {deleted} old records")
    except:
        session.rollback()

if __name__ == "__main__":
    try:
        step_start = time.time()
        stock_data, failed_symbols = fetch_stock_prices()
        log(f"[TIMING] fetch_stock_prices: {time.time() - step_start:.1f}s")
        
        step_start = time.time()
        save_stock_prices(stock_data)
        log(f"[TIMING] save_stock_prices: {time.time() - step_start:.1f}s")
        
        if failed_symbols:
            step_start = time.time()
            quarantine_symbols(failed_symbols)
            log(f"[TIMING] quarantine_symbols: {time.time() - step_start:.1f}s")
        
        try:
            log("Starting alert check...")
            step_start = time.time()
            from alerts import check_price_alerts
            log(f"[TIMING] import alerts: {time.time() - step_start:.1f}s")
            
            step_start = time.time()
            check_price_alerts(session, StockPrice)
            log(f"[TIMING] check_price_alerts: {time.time() - step_start:.1f}s")
        except Exception as e:
            log(f"Alert check error: {e}")
            import traceback
            traceback.print_exc()
        
        elapsed = time.time() - start_time
        log(f"Done in {elapsed:.0f}s")
        
    except Exception as e:
        log(f"Error: {e}")
        import traceback
        traceback.print_exc()
        if stock_data_global:
            log(f"Saving {len(stock_data_global)} prices before exit...")
            save_stock_prices(stock_data_global)
