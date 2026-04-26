from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, split, regexp_replace, lower, from_unixtime, count, avg, length
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 1. Initialize Spark Engine
spark = SparkSession.builder.appName("HourlyWordSentiment").getOrCreate()
analyzer = SentimentIntensityAnalyzer()

def get_sentiment_score(text):
    if text is None:
        return 0.0
    return analyzer.polarity_scores(text)['compound']

sentiment_udf = udf(get_sentiment_score, FloatType())

print("Loading Data...")

# 2. Load the Bronze Data
data_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\reddit_data\*.jsonl"
reddit_df = spark.read.json(data_path)

print("Processing Hourly Timestamps and Sentiment...")

# 3. The Hourly Time-Series Conversion
# By passing "yyyy-MM-dd HH:00:00" to from_unixtime, Spark automatically 
# locks the timestamp to the top of the hour (e.g., 2:45 PM becomes 2:00 PM).
processed_df = reddit_df.withColumn("hour_bucket", from_unixtime(col("created_utc"), "yyyy-MM-dd HH:00:00")) \
                        .withColumn("post_sentiment", sentiment_udf(col("title")))

# 4. Clean and Extract Words
clean_text = regexp_replace(lower(col("title")), r"[^a-z\s]", "")
words_array = split(clean_text, r"\s+")

# 5. Explode and Filter
exploded_df = processed_df.select(
    col("hour_bucket"),
    col("post_sentiment"),
    explode(words_array).alias("word")
).filter(length(col("word")) > 3)

print("Aggregating Hourly Data...")

# 6. The Hourly Aggregation
time_series_df = exploded_df.groupBy("hour_bucket", "word") \
                            .agg(
                                count("*").alias("hourly_mentions"),
                                avg("post_sentiment").alias("avg_hourly_sentiment")
                            )

print("Writing Hourly JSON...")

# 7. Write to Gold Layer
output_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\output\hourly_series_data"
time_series_df.coalesce(1).write.mode("overwrite").json(output_path)

print(f"Success! Hourly file saved to: {output_path}")

spark.stop()