import pandas as pd
import psycopg2
import os

# 1. Define paths
file_path = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/gold/vaderSentimentAnalysisFinal"
csv_temp_path = "/mnt/d/Bluesky-Reddit-BigDataProject/app/temp_word_series.csv"

print("1. Reading 18 million rows from Parquet (takes a few seconds)...")
df = pd.read_parquet(file_path, engine='pyarrow')

print("2. Scrubbing NUL characters...")
df['word'] = df['word'].str.replace('\x00', '', regex=False)

# Force the exact column order so PostgreSQL doesn't get confused
df = df[['word', 'time_range', 'avg_vader_sentiment_score', 'word_count']]

print("3. Writing to temporary CSV... (This is the longest step, give it a minute or two)")
df.to_csv(csv_temp_path, index=False)

print("4. Firehosing data into PostgreSQL using native COPY...")
# Connect directly using psycopg2
conn = psycopg2.connect(
    dbname="bluesky_db",
    user="backend_user",
    password="supersecretpassword",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Clear the table first so we don't duplicate data
cur.execute("TRUNCATE TABLE word_time_series;")

# The magic COPY command
with open(csv_temp_path, 'r') as f:
    # copy_expert streams the file directly into the database engine
    sql = """
    COPY word_time_series (word, time_range, avg_vader_sentiment_score, word_count) 
    FROM STDIN WITH CSV HEADER
    """
    cur.copy_expert(sql, f)

conn.commit()
cur.close()
conn.close()

print("5. Cleaning up temp file...")
os.remove(csv_temp_path)

print("✅ Boom. 18 million rows loaded in record time!")