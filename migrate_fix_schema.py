"""
Migration script to fix stock_symbols table schema.
Drops the old table and recreates with correct schema.
"""
import os
from sqlalchemy import create_engine, text
from models import Base

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL environment variable set")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://")

if "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require" if "?" not in DATABASE_URL else "&sslmode=require"

engine = create_engine(DATABASE_URL)

print("Dropping old stock_symbols table...")
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS stock_symbols CASCADE"))
    conn.commit()
print("Done.")

print("Creating new stock_symbols table with correct schema...")
Base.metadata.create_all(engine)
print("Done.")

print("\nMigration complete! Now run update_symbols.py to populate the table.")
