"""Create all required tables in the Bluesky PostgreSQL database."""
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load the central .env from the project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

db_url = os.getenv("BLUESKY_DB_URL", "postgresql://backend_user:supersecretpassword@localhost:5432/bluesky_db")
engine = create_engine(db_url)

sql_file = Path(__file__).resolve().parent / "backend" / "create_tables.sql"
sql_content = sql_file.read_text(encoding="utf-8")

print(f"Connecting to: {db_url}")
print(f"Running SQL from: {sql_file}\n")

with engine.connect() as conn:
    for statement in sql_content.split(";"):
        statement = statement.strip()
        if statement:
            print(f"  -> {statement[:60]}...")
            conn.execute(text(statement))
    conn.commit()

print("\n✅ All tables created successfully!")
