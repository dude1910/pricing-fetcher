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

migrations = [
    ("stock_symbols", "quarantined_until", "TIMESTAMP"),
    ("stock_prices", "volume", "BIGINT"),
    ("alert_history", "volume", "BIGINT"),
    ("alert_history", "volume_ratio", "FLOAT"),
]

with engine.connect() as conn:
    for table, column, col_type in migrations:
        result = conn.execute(text(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table}' 
            AND column_name = '{column}'
        """))
        
        if result.fetchone() is None:
            print(f"Adding {table}.{column}...")
            try:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
                conn.commit()
                print(f"  Added!")
            except Exception as e:
                print(f"  Skipped: {e}")
        else:
            print(f"{table}.{column} exists")
    
    print("Migration complete!")
