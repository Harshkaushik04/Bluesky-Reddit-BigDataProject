from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Added caseSensitive to handle JSON type column quirks
builder = SparkSession.builder.appName("Dashboard_Table2_Ingestion") \
    .master("local[2]") \
    .config("spark.driver.memory", "5g") \
    .config("spark.sql.files.maxPartitionBytes", "16777216") \
    .config("spark.sql.caseSensitive", "true")

spark = builder.getOrCreate()

# 1. Process Firehose Data 
firehose_df = spark.read.format("parquet") \
    .load("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/bronze/initial_firehose_parquet")

# Firehose DOES use time_us, so this stays exactly the same
firehose_df = firehose_df.withColumn("timestamp", timestamp_micros(col("time_us")))

firehose_counts = firehose_df.groupBy(window(col("timestamp"), "2 hours").alias("time_window")) \
    .count() \
    .withColumn("source_type", lit("firehose")) \
    .select(col("time_window.start").alias("time_bucket"), "source_type", col("count").alias("record_count"))

# 2. Process getPosts Data
getPosts_df = spark.read.format("json") \
    .load("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/silver/getPosts/posts_results.jsonl")

# FIXED: getPosts uses record.createdAt, NOT time_us
getPosts_df = getPosts_df.withColumn("timestamp", to_timestamp(col("record.createdAt")))

getPosts_counts = getPosts_df.groupBy(window(col("timestamp"), "2 hours").alias("time_window")) \
    .count() \
    .withColumn("source_type", lit("getPosts_endpoint")) \
    .select(col("time_window.start").alias("time_bucket"), "source_type", col("count").alias("record_count"))

# 3. Combine both streams into one table
final_ingestion_df = firehose_counts.unionByName(getPosts_counts, allowMissingColumns=True)

# 4. Save
final_ingestion_df.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/gold/ingestion_metrics_timeline")