from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip
from pyspark.ml.feature import StopWordsRemover

# 1. Initialize Spark Session Builder
builder = SparkSession.builder.appName("Local_Lakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.sql.caseSensitive", "true") \
    .master("local[4]")

# 2. Create Spark Session
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Multiple stopword sources: Spark default + custom frequent fillers
DEFAULT_STOPWORDS = set(StopWordsRemover.loadDefaultStopWords("english"))
CUSTOM_STOPWORDS = {
    "rt", "amp", "via", "http", "https", "www", "com", "org", "net", "co",
    "im", "ive", "dont", "didnt", "doesnt", "cant", "couldnt", "wouldnt",
    "shouldnt", "youre", "theyre", "weve", "thats", "its", "u", "ur", "ya",
    "lol", "lmao", "omg", "idk", "btw", "thx", "pls", "okay", "ok", "one",
}
ALL_STOPWORDS = sorted(DEFAULT_STOPWORDS.union(CUSTOM_STOPWORDS))
STOPWORDS_ARRAY = array(*[lit(w) for w in ALL_STOPWORDS])

# 3. Read Data
df = spark.read \
    .format("parquet") \
    .load("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/silver/vaderSentimentTable/")

# 4. Transformations
# Filter for English language
df = df.filter(col("commit.record.langs") == array(lit("en")))

# Convert timestamp
df = df.withColumn("timestamp", timestamp_micros(col("time_us")))

# Split and explode text into individual words
df = df.withColumn("splitted_text", split(lower(regexp_replace(col("commit.record.text"), r"[^a-zA-Z0-9\s]", " ")), r"\s+"))
df = df.withColumn("filtered_tokens", array_except(col("splitted_text"), STOPWORDS_ARRAY))
df = df.withColumn("filtered_tokens", expr("filter(filtered_tokens, x -> x rlike '^[a-z][a-z0-9]{1,}$')"))
df = df.withColumn("exploded_text", explode(col("filtered_tokens")))

# Group by word and 2-hour time window, calculate average sentiment and word count
df = df.groupBy(
    col("exploded_text").alias("word"),
    window(col("timestamp"), "10 minutes").alias("time_window") # Aliased as time_window temporarily
).agg(
    avg(col("vader_sentiment_score")).alias("avg_vader_sentiment_score"),
    count("*").alias("word_count")
).select(
    col("word"),
    col("time_window.start").alias("time_range"), # Unpacks the struct and renames to match SQL
    col("avg_vader_sentiment_score"),
    col("word_count")
)

# 5. Write Data
df.write.format("parquet") \
    .mode("append") \
    .save("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/gold/vaderSentimentAnalysisFinal/")