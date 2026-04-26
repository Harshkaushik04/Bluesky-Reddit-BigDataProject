from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ADDED: spark.sql.caseSensitive to prevent the JSON "type" column crash
builder = SparkSession.builder.appName("Dashboard_Table4_RedditLinks") \
    .master("local[2]") \
    .config("spark.driver.memory", "5g") \
    .config("spark.sql.files.maxPartitionBytes", "16777216") \
    .config("spark.sql.caseSensitive", "true")

spark = builder.getOrCreate()

# 1. Load Data
getPosts_df = spark.read.format("json") \
    .load("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/silver/getPosts/posts_results.jsonl")

# 2. Hard filter: ONLY keep rows that actually mention reddit.com
# This drops 99% of the data instantly, saving massive memory
reddit_mentions_df = getPosts_df.filter(lower(col("record.text")).contains("reddit.com"))

# 3. Explode into topics
reddit_topics_df = reddit_mentions_df.withColumn(
    "topic_name", explode(array_distinct(split(lower(col("record.text")), r"\s+")))
)

# 4. Group by Topic and count occurrences
reddit_stats_df = reddit_topics_df.filter(~col("topic_name").contains("reddit.com")) \
    .groupBy("topic_name") \
    .count() \
    .withColumnRenamed("count", "reddit_link_count")

# 5. Save
reddit_stats_df.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/gold/reddit_crossover_stats")