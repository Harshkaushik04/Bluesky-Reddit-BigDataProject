import pandas as pd
import psycopg2
import os

# 1. Define paths
file_path = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/gold/vaderSentimentAnalysisFinal"
csv_temp_path = "/mnt/d/Bluesky-Reddit-BigDataProject/app/temp_word_series.csv"

print("1. Reading Parquet folder...")
df = pd.read_parquet(file_path, engine="pyarrow")

# 2. Shrink data to 10% to save time and space
print("2. Shrinking data to 10% via random sampling...")
df = df.sample(frac=0.2, random_state=42)

# 3. Clean the data (Fixing the NUL byte error)
print("3. Scrubbing NUL characters...")
if "word" in df.columns:
    df["word"] = df["word"].str.replace("\x00", "", regex=False)

# Force the exact column order to match the Postgres schema perfectly
df = df[["word", "time_range", "avg_vader_sentiment_score", "word_count"]]

# 4. Write to temporary CSV for the fast COPY command
print("4. Writing to temporary CSV... (Should be very fast now)")
df.to_csv(csv_temp_path, index=False)

print("5. Connecting to PostgreSQL...")
# 5. Connect directly using psycopg2
conn = psycopg2.connect(
    dbname="bluesky_db",
    user="backend_user",
    password="supersecretpassword",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# --- THE FIX: Create the table if it got deleted or never existed ---
print("   -> Ensuring table exists...")
create_table_sql = """
CREATE TABLE IF NOT EXISTS word_time_series (
    word TEXT,
    time_range TIMESTAMP WITHOUT TIME ZONE,
    avg_vader_sentiment_score FLOAT,
    word_count BIGINT
);
"""
cur.execute(create_table_sql)
# --------------------------------------------------------------------

# This is the Overwrite command: it instantly wipes the old data clean
print("   -> Emptying existing table...")
cur.execute("TRUNCATE TABLE word_time_series;")

# The magic COPY command
print("   -> Firehosing data into PostgreSQL using native COPY...")
with open(csv_temp_path, "r", encoding="utf-8") as f:
    sql = """
    COPY word_time_series (word, time_range, avg_vader_sentiment_score, word_count) 
    FROM STDIN WITH CSV HEADER
    """
    cur.copy_expert(sql, f)

conn.commit()
cur.close()
conn.close()

# 6. Clean up
print("6. Cleaning up temp file...")
if os.path.exists(csv_temp_path):
    os.remove(csv_temp_path)

print("Done! Your table is now fully loaded, clean, and ready for FastAPI.")