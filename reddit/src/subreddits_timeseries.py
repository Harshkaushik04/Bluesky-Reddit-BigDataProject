from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, count, desc

# 1. Initialize Spark Engine (Keeping the 4GB RAM safeguard!)
spark = SparkSession.builder \
    .appName("TimeSeriesSubreddits") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("Loading Bronze Data...")

# 2. Load the Raw Data
data_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\reddit_data\*.jsonl"
reddit_df = spark.read.json(data_path)

print("Processing Hourly Timestamps and Subreddits...")

# 3. Extract the Time and the Subreddit
# We use the same hourly bucket trick as before
# The standard Reddit API stores the community name in the "subreddit" column
processed_df = reddit_df.withColumn("hour_bucket", from_unixtime(col("created_utc"), "yyyy-MM-dd HH:00:00")) \
                        .select("hour_bucket", "subreddit") \
                        .filter(col("subreddit").isNotNull()) # Filter out any corrupted rows

print("Aggregating Subreddit Volume over Time...")

# 4. The Big Data Aggregation
# Group by the specific hour AND the subreddit, then count how many posts occurred
time_series_subreddits = processed_df.groupBy("hour_bucket", "subreddit") \
                                     .agg(count("*").alias("post_count")) \
                                     .orderBy("hour_bucket", desc("post_count"))

print("Writing to Output Folder...")

# 5. Save the Gold Data to your clean output directory
output_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\output\time_series_subreddits"
time_series_subreddits.coalesce(1).write.mode("overwrite").json(output_path)

print(f"Success! Subreddit Time-Series saved to: {output_path}")

# 6. Instant Verification
print("\n--- 📊 Sneak Peek: Subreddit Volume ---")
time_series_subreddits.show(10, truncate=False)

# Clean Shutdown
spark.stop()