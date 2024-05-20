import requests
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, StockSymbol

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL environment variable set")

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

API_TOKEN = os.environ.get('API_TOKEN')
EXCHANGES = ['HM', 'PA', 'US']
API_URL = 'https://eodhd.com/api/exchange-symbol-list/{exchange_code}?api_token=' + API_TOKEN

def fetch_symbols(exchange_code):
    response = requests.get(API_URL.format(exchange_code=exchange_code))
    data = response.text.split("\n")
    symbols = []
    for line in data:
        parts = line.split(",")
        if len(parts) > 1:
            symbol = parts[0].strip('"')
            name = parts[1].strip('"')
            symbols.append((symbol, name))
    return symbols

def save_symbols(symbols, exchange):
    for symbol, name in symbols:
        existing_symbol = session.query(StockSymbol).filter_by(symbol=symbol, exchange=exchange).first()
        if not existing_symbol:
            new_symbol = StockSymbol(symbol=symbol, name=name, exchange=exchange)
            session.add(new_symbol)
    session.commit()

def update_symbols():
    for exchange in EXCHANGES:
        symbols = fetch_symbols(exchange)
        save_symbols(symbols, exchange)

if __name__ == "__main__":
    update_symbols()
