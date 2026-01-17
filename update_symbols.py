"""
Update stock symbols from NASDAQ FTP feed.
Optimized for speed with bulk inserts.
"""
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, StockSymbol

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
print("Database connected.")


def fetch_nasdaq_symbols():
    """Fetch symbols from NASDAQ trader FTP feeds."""
    print("Fetching NASDAQ listed symbols...")
    
    nasdaq_url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    other_url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    
    all_symbols = []
    
    try:
        nasdaq_df = pd.read_csv(nasdaq_url, sep='|')
        nasdaq_df = nasdaq_df[nasdaq_df['Test Issue'] == 'N']
        nasdaq_df = nasdaq_df[~nasdaq_df['Symbol'].str.contains(r'[\$\.]', na=False)]
        
        for _, row in nasdaq_df.iterrows():
            if pd.notna(row['Symbol']) and pd.notna(row['Security Name']):
                all_symbols.append({
                    'symbol': str(row['Symbol']).strip(),
                    'name': str(row['Security Name']).strip()[:200],
                    'exchange': 'NASDAQ'
                })
        print(f"  Found {len(all_symbols)} NASDAQ symbols")
    except Exception as e:
        print(f"  Error fetching NASDAQ: {e}")
    
    try:
        other_df = pd.read_csv(other_url, sep='|')
        other_df = other_df[other_df['Test Issue'] == 'N']
        other_df = other_df[~other_df['ACT Symbol'].str.contains(r'[\$\.]', na=False)]
        
        other_count = 0
        for _, row in other_df.iterrows():
            if pd.notna(row['ACT Symbol']) and pd.notna(row['Security Name']):
                exchange = str(row['Exchange']).strip() if pd.notna(row.get('Exchange')) else 'OTHER'
                all_symbols.append({
                    'symbol': str(row['ACT Symbol']).strip(),
                    'name': str(row['Security Name']).strip()[:200],
                    'exchange': exchange
                })
                other_count += 1
        print(f"  Found {other_count} other exchange symbols")
    except Exception as e:
        print(f"  Error fetching other exchanges: {e}")
    
    print(f"Total symbols found: {len(all_symbols)}")
    return all_symbols


def save_symbols_to_db(symbols_list):
    """Save symbols to database using FAST bulk insert."""
    print("Inserting symbols with bulk insert...")
    
    # Create StockSymbol objects
    objects = [
        StockSymbol(
            symbol=item['symbol'],
            name=item['name'],
            exchange=item['exchange']
        )
        for item in symbols_list
    ]
    
    # Bulk insert all at once
    try:
        session.bulk_save_objects(objects)
        session.commit()
        print(f"Inserted {len(objects)} symbols successfully!")
    except Exception as e:
        session.rollback()
        print(f"Bulk insert failed: {e}")
        print("Falling back to individual inserts...")
        
        # Fallback: insert one by one, skip duplicates
        added = 0
        for item in symbols_list[:1000]:  # Limit to 1000 for speed
            try:
                existing = session.query(StockSymbol).filter_by(
                    symbol=item['symbol']
                ).first()
                if not existing:
                    session.add(StockSymbol(**item))
                    added += 1
            except:
                pass
        session.commit()
        print(f"Added {added} symbols via fallback")


def update_symbols():
    """Main function to update symbols."""
    print("Starting symbol update...")
    symbols = fetch_nasdaq_symbols()
    
    if symbols:
        save_symbols_to_db(symbols)
        print("Symbol update complete!")
    else:
        print("No symbols fetched - something went wrong")


if __name__ == "__main__":
    update_symbols()
