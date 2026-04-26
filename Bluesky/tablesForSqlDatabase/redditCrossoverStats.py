from pyspark.sql import SparkSession
from pyspark.sql.functions import *

builder = SparkSession.builder.appName("Dashboard_Table4_RedditLinks") \
    .master("local[2]") \
    .config("spark.driver.memory", "5g") \
    .config("spark.sql.files.maxPartitionBytes", "16777216") \
    .config("spark.sql.caseSensitive", "true")

spark = builder.getOrCreate()

# 1. Load Data
getPosts_df = spark.read.format("json") \
    .load("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/silver/getPosts/posts_results.jsonl")

# 2. Extract Timestamp
getPosts_df = getPosts_df.withColumn("timestamp", to_timestamp(col("record.createdAt")))

# 3. Hard filter: ONLY keep rows that actually mention reddit.com
reddit_mentions_df = getPosts_df.filter(lower(col("record.text")).contains("reddit.com"))

# 4. Explode into topics
reddit_topics_df = reddit_mentions_df.withColumn(
    "topic_name", explode(array_distinct(split(lower(col("record.text")), r"\s+")))
)

# 5. Group by 2-Hour Time Bucket and Topic
reddit_stats_df = reddit_topics_df.filter(~col("topic_name").contains("reddit.com")) \
    .groupBy(window(col("timestamp"), "2 hours").alias("time_window"), "topic_name") \
    .count() \
    .select(
        "topic_name", 
        col("time_window.start").alias("time_bucket"), # Extracts just the start time for Postgres compatibility
        col("count").alias("reddit_link_count")
    )

# 6. Save
reddit_stats_df.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/gold/reddit_crossover_stats")