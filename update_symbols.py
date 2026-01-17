"""
Update stock symbols from NASDAQ FTP feed.
This replaces the broken pyfinviz approach.
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


def fetch_nasdaq_symbols():
    """Fetch symbols from NASDAQ trader FTP feeds."""
    print("Fetching NASDAQ listed symbols...")
    
    # NASDAQ listed stocks
    nasdaq_url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    # Other listed stocks (NYSE, etc)
    other_url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    
    all_symbols = []
    
    try:
        # NASDAQ symbols
        nasdaq_df = pd.read_csv(nasdaq_url, sep='|')
        nasdaq_df = nasdaq_df[nasdaq_df['Test Issue'] == 'N']  # Exclude test issues
        nasdaq_df = nasdaq_df[~nasdaq_df['Symbol'].str.contains(r'[\$\.]', na=False)]  # Exclude special symbols
        
        for _, row in nasdaq_df.iterrows():
            if pd.notna(row['Symbol']) and pd.notna(row['Security Name']):
                all_symbols.append({
                    'symbol': str(row['Symbol']).strip(),
                    'name': str(row['Security Name']).strip()[:200],  # Truncate long names
                    'exchange': 'NASDAQ'
                })
        print(f"  Found {len(all_symbols)} NASDAQ symbols")
    except Exception as e:
        print(f"  Error fetching NASDAQ: {e}")
    
    try:
        # Other exchanges (NYSE, AMEX, etc)
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
    """Save symbols to database, skipping duplicates."""
    added = 0
    skipped = 0
    
    for item in symbols_list:
        try:
            existing = session.query(StockSymbol).filter_by(
                symbol=item['symbol'], 
                exchange=item['exchange']
            ).first()
            
            if not existing:
                new_symbol = StockSymbol(
                    symbol=item['symbol'],
                    name=item['name'],
                    exchange=item['exchange']
                )
                session.add(new_symbol)
                added += 1
                
                # Commit in batches
                if added % 500 == 0:
                    session.commit()
                    print(f"  Added {added} symbols so far...")
            else:
                skipped += 1
        except Exception as e:
            print(f"  Error adding {item['symbol']}: {e}")
            session.rollback()
    
    session.commit()
    print(f"Added {added} new symbols, skipped {skipped} existing")


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
