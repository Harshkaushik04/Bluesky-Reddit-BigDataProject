import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import StopWordsRemover

builder = SparkSession.builder.appName("Update_Gold_Table3_RedditLinks") \
    .master("local[2]") \
    .config("spark.driver.memory", "5g") \
    .config("spark.sql.caseSensitive", "true")

spark = builder.getOrCreate()

# 1. Define Stopwords List
DEFAULT_STOPWORDS = set(StopWordsRemover.loadDefaultStopWords("english"))
CUSTOM_STOPWORDS = {
    "rt", "amp", "via", "http", "https", "www", "com", "org", "net", "co",
    "im", "ive", "dont", "didnt", "doesnt", "cant", "couldnt", "wouldnt",
    "shouldnt", "youre", "theyre", "weve", "thats", "its", "u", "ur", "ya",
    "lol", "lmao", "omg", "idk", "btw", "thx", "pls", "okay", "ok", "one",
}
ALL_STOPWORDS = list(sorted(DEFAULT_STOPWORDS.union(CUSTOM_STOPWORDS)))

# 2. Define Paths
original_path = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/gold/reddit_crossover_stats"
temp_path = original_path + "_temp"

# 3. Load & Filter Data
df = spark.read.format("parquet").load(original_path)
df_filtered = df.filter(~col("topic_name").isin(ALL_STOPWORDS))

# 4. Save to Temp Path
df_filtered.write.format("parquet") \
    .mode("overwrite") \
    .save(temp_path)

# 5. Swap Directories for In-Place Update
if os.path.exists(original_path):
    shutil.rmtree(original_path)
os.rename(temp_path, original_path)