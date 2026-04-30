import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load the central .env from the project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# 1. Connect to your PostgreSQL database
db_url = os.getenv("BLUESKY_DB_URL", "postgresql://backend_user:supersecretpassword@localhost:5432/bluesky_db")
engine = create_engine(db_url)

# 2. Define the paths to your 4 Gold Parquet folders
base_path = str(Path(os.getenv("BLUESKY_DATA_DIR", Path(__file__).resolve().parent.parent / "Bluesky_data")).resolve() / "gold")

tables_to_load = {
    "word_time_series": os.path.join(base_path, "vaderSentimentAnalysisFinal"),
    "ingestion_metrics_timeline": os.path.join(base_path, "ingestion_metrics_timeline"),
    "controversial_topics_timeline": os.path.join(base_path, "controversial_topics_timeline"),
    "reddit_crossover_stats": os.path.join(base_path, "reddit_crossover_stats")
}

print("Starting database ingestion...\n")

# 3. Loop through and load each table
for table_name, file_path in tables_to_load.items():
    try:
        print(f"Loading Parquet data from {file_path}...")
        df = pd.read_parquet(file_path, engine='pyarrow')
        
        # --- NEW CODE: Scrub null bytes from the text column ---
        if table_name == "word_time_series" and 'word' in df.columns:
            print("Scrubbing NUL characters from text...")
            df['word'] = df['word'].str.replace('\x00', '', regex=False)
        # -------------------------------------------------------

        print(f"Pushing {len(df)} rows to PostgreSQL table: '{table_name}'...")
        # Write to Postgres
        df.to_sql(
            table_name, 
            engine, 
            if_exists='replace', 
            index=False, 
            chunksize=5000, 
            method='multi'
        )
        print(f"Successfully loaded {table_name}!\n")
        
    except Exception as e:
        print(f"Error loading {table_name}: {e}\n")

print("All dashboard tables are now live in PostgreSQL!")