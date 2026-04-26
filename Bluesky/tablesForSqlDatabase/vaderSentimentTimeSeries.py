from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip

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
df = df.withColumn("splitted_text", split(col("commit.record.text"), r"\s+"))
df = df.withColumn("exploded_text", explode(col("splitted_text")))

# Group by word and 2-hour time window, calculate average sentiment and word count
df = df.groupBy(
    col("exploded_text").alias("word"),
    window(col("timestamp"), "2 hours").alias("time_range")
).agg(
    avg(col("vader_sentiment_score")).alias("avg_vader_sentiment_score"),
    count("*").alias("word_count")
)

# 5. Write Data
df.write.format("parquet") \
    .mode("append") \
    .save("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/silver/vaderSentimentAnalysisFinal/")