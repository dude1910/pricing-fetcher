import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
import os
from datetime import datetime, timezone, timedelta
from models import StockSymbol  

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

engine = create_engine(DATABASE_URL)
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
        url = f"{YAHOO_URL}/{symbol}"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        price_element = soup.select_one('fin-streamer[data-field="regularMarketPrice"]')
        print(f"Price element: {price_element}")
        if price_element:
            stock_data.append({
                "symbol": symbol,
                "name": name,
                "price": float(price_element["data-value"])
            })
        else:
            print(f"Error fetching data for symbol: {symbol}")
    return stock_data

def save_stock_prices(stock_data):
    for entry in stock_data:
        stock_price = StockPrice(symbol=entry["symbol"], name=entry["name"], price=entry["price"])
        session.add(stock_price)
    session.commit()
    
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
    session.query(StockPrice).filter(StockPrice.timestamp < cutoff_time).delete(synchronize_session=False)
    
    session.commit()

if __name__ == "__main__":
    stock_data = fetch_stock_prices()
    save_stock_prices(stock_data)
