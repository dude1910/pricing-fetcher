from flask import Flask, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from models import Base, StockSymbol

app = Flask(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL environment variable set")

print(f"database url: {DATABASE_URL}")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

@app.route('/stocks', methods=['GET'])
def get_stocks():
    stocks = session.query(StockSymbol).all()
    return jsonify([{"symbol": stock.symbol, "name": stock.name, "exchange": stock.exchange} for stock in stocks])

if __name__ == "__main__":
    app.run(debug=True)
