from pyfinviz.screener import Screener
import pandas as pd
import os
from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class StockSymbol(Base):
    __tablename__ = 'stock_symbols'
    symbol = Column(String, primary_key=True)
    name = Column(String)
    exchange = Column(String)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL environment variable set")

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

def fetch_tickers_from_finviz(pages_to_fetch=10):
    screener = Screener(pages=[x for x in range(1, pages_to_fetch + 1)])
    data = screener.data_frames
    all_data = [page for page in data.values()]
    tickers_df = pd.concat(all_data, ignore_index=True)
    return tickers_df[['Ticker', 'Company']]

# Save tickers to the database
def save_symbols_to_db(tickers_df, exchange):
    for index, row in tickers_df.iterrows():
        symbol = row['Ticker']
        name = row['Company']
        existing_symbol = session.query(StockSymbol).filter_by(symbol=symbol, exchange=exchange).first()
        if not existing_symbol:
            new_symbol = StockSymbol(symbol=symbol, name=name, exchange=exchange)
            session.add(new_symbol)
    session.commit()

def update_symbols():
    exchange = 'NASDAQ'  
    tickers_df = fetch_tickers_from_finviz(pages_to_fetch=600)
    save_symbols_to_db(tickers_df, exchange)

if __name__ == "__main__":
    update_symbols()
