from pyspark.sql.functions import (
    array_contains, avg, coalesce, col, count, explode, lit, split, timestamp_micros, udf, window
)
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from common import CHECKPOINT_DIR, GOLD_DIR, POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, build_spark

# Initialize VADER analyzer globally so it doesn't re-initialize for every single word
analyzer = SentimentIntensityAnalyzer()

@udf(returnType=FloatType())
def calculate_vader(text):
    if not text:
        return 0.0
    return analyzer.polarity_scores(text).get("compound", 0.0)


def write_batch(batch_df, _batch_id: int):
    if batch_df.isEmpty():
        return
    batch_df.write.mode("append").parquet(str(GOLD_DIR / "word_time_series"))
    batch_df.write.mode("append").jdbc(
        POSTGRES_JDBC_URL, "word_time_series", properties=POSTGRES_PROPERTIES
    )


def main():
    spark = build_spark("StructuredStreaming_WordTimeSeries")
    
    # Read raw JSON records directly from the firehose output
    source_df = (
        spark.readStream.format("json")
        .load("/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/initial_firehose")
    )

    transformed = (
        # Safely check if 'en' is in the languages array
        source_df.filter(array_contains(col("commit.record.langs"), "en"))
        .withColumn("timestamp", timestamp_micros(col("time_us")))
        .withColumn("text_clean", coalesce(col("commit.record.text"), lit("")))
        .filter(col("text_clean") != "")
        # Calculate sentiment on the full post BEFORE splitting into words
        .withColumn("vader_sentiment_score", calculate_vader(col("text_clean")))
        .withColumn("word", explode(split(col("text_clean"), r"\s+")))
        .filter(col("word") != "")
        .groupBy(col("word"), window(col("timestamp"), "10 minutes").alias("time_window"))
        .agg(
            avg(col("vader_sentiment_score")).alias("avg_vader_sentiment_score"),
            count("*").alias("word_count"),
        )
        .select(
            col("word"),
            col("time_window.start").alias("time_range"),
            col("avg_vader_sentiment_score"),
            col("word_count"),
        )
    )

    query = (
        transformed.writeStream.outputMode("update")
        .option("checkpointLocation", str(CHECKPOINT_DIR / "word_time_series"))
        .foreachBatch(write_batch)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()