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

for table, column, col_type in migrations:
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table}' 
                AND column_name = '{column}'
            """))
            
            if result.fetchone() is None:
                table_exists = conn.execute(text(f"""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_name = '{table}'
                """)).fetchone()
                
                if table_exists:
                    print(f"Adding {table}.{column}...")
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
                    conn.commit()
                    print(f"  Added!")
                else:
                    print(f"{table} doesn't exist yet, skipping {column}")
            else:
                print(f"{table}.{column} exists")
    except Exception as e:
        print(f"Skipped {table}.{column}: {e}")

print("Migration complete!")
