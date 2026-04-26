from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

builder = SparkSession.builder.appName("Dashboard_Table3_Controversial") \
    .master("local[2]") \
    .config("spark.driver.memory", "5g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "1g") \
    .config("spark.sql.files.maxPartitionBytes", "16777216") \
    .config("spark.sql.caseSensitive", "true")

spark = builder.getOrCreate()

# 1. Load Data
getPosts_df = spark.read.format("json") \
    .load("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/silver/getPosts/posts_results.jsonl")

# 2. Filter English (FIXED: Removed "commit.")
getPosts_df = getPosts_df.filter(col("record.langs") == array(lit("en")))

# 3. Native Spark Math for Ratio
controversialPosts_df = getPosts_df.withColumn(
    "like_to_comment_ratio",
    when(col("replyCount") == 0, lit(0.0)).otherwise(col("likeCount") / col("replyCount"))
)

# 4. Filter thresholds
controversialPosts_df = controversialPosts_df.filter((col("like_to_comment_ratio") != 0) & (col("replyCount") >= 50))

# 5. Extract Timestamp and create a 10-Minute Time Bucket (FIXED: using window)
controversialPosts_df = controversialPosts_df.withColumn("timestamp", to_timestamp(col("indexedAt"))) \
    .withColumn("time_bucket", window(col("timestamp"), "10 minutes").start)

# 6. Text Processing (This was already correctly pointing to record.text)
controversialPosts_df = controversialPosts_df.withColumn(
    "topic_name", explode(array_distinct(split(lower(col("record.text")), r"\s+")))
)

# 7. Group By BOTH Time Bucket and Topic Name
controversial_final_df = controversialPosts_df.groupBy("time_bucket", "topic_name") \
    .agg(avg(col("like_to_comment_ratio")).alias("average_like_to_comment_ratio"))

# 8. Save
controversial_final_df.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/gold/controversial_topics_timeline")