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
    "ALTER TABLE alert_outcomes ADD COLUMN IF NOT EXISTS trade_result FLOAT",
    "ALTER TABLE alert_outcomes ADD COLUMN IF NOT EXISTS trade_exit_reason VARCHAR",
    "ALTER TABLE alert_outcomes ADD COLUMN IF NOT EXISTS trade_max_gain FLOAT",
    "ALTER TABLE alert_outcomes ADD COLUMN IF NOT EXISTS trade_max_drawdown FLOAT",
    "ALTER TABLE alert_outcomes ADD COLUMN IF NOT EXISTS trade_hold_minutes INTEGER",
    "ALTER TABLE alert_outcomes ADD COLUMN IF NOT EXISTS trade_checked BOOLEAN DEFAULT FALSE",
]

with engine.connect() as conn:
    for sql in migrations:
        try:
            conn.execute(text(sql))
            conn.commit()
            print(f"OK: {sql[:60]}...")
        except Exception as e:
            print(f"Skip: {e}")

print("\nMigration complete!")
