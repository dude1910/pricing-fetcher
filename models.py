from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, UniqueConstraint

Base = declarative_base()

class StockSymbol(Base):
    __tablename__ = 'stock_symbols'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False)
    name = Column(String, nullable=True)
    exchange = Column(String, nullable=False)
    quarantined_until = Column(DateTime, nullable=True)
    __table_args__ = (UniqueConstraint("symbol", "exchange", name="_symbol_exchange_uc"),)

