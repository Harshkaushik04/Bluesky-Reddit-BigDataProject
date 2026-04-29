import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip
from pyspark.ml.feature import StopWordsRemover

# 1. Initialize Spark Session Builder 
builder = SparkSession.builder.appName("Update_Gold_Table4_Vader") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.caseSensitive", "true") \
    .master("local[4]")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 2. Define Stopwords List
DEFAULT_STOPWORDS = set(StopWordsRemover.loadDefaultStopWords("english"))
CUSTOM_STOPWORDS = {
    "rt", "amp", "via", "http", "https", "www", "com", "org", "net", "co",
    "im", "ive", "dont", "didnt", "doesnt", "cant", "couldnt", "wouldnt",
    "shouldnt", "youre", "theyre", "weve", "thats", "its", "u", "ur", "ya",
    "lol", "lmao", "omg", "idk", "btw", "thx", "pls", "okay", "ok", "one",
}
ALL_STOPWORDS = list(sorted(DEFAULT_STOPWORDS.union(CUSTOM_STOPWORDS)))

# 3. Define Paths
original_path = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/gold/vaderSentimentAnalysisFinal"
temp_path = original_path + "_temp"

# 4. Load & Filter Data
df = spark.read.format("parquet").load(original_path)
df_filtered = df.filter(~col("word").isin(ALL_STOPWORDS))

# 5. Save to Temp Path
df_filtered.write.format("parquet") \
    .mode("overwrite") \
    .save(temp_path)

# 6. Swap Directories for In-Place Update
if os.path.exists(original_path):
    shutil.rmtree(original_path)
os.rename(temp_path, original_path)