"""
Add quarantined_until column to stock_symbols table.
Run this once to migrate the database.
"""
import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL environment variable set")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://")

if "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require" if "?" not in DATABASE_URL else "&sslmode=require"

engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'stock_symbols' 
        AND column_name = 'quarantined_until'
    """))
    
    if result.fetchone() is None:
        print("Adding quarantined_until column...")
        conn.execute(text("""
            ALTER TABLE stock_symbols 
            ADD COLUMN quarantined_until TIMESTAMP
        """))
        conn.commit()
        print("Column added successfully!")
    else:
        print("Column already exists, skipping.")
