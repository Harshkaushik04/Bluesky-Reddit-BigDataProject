from pyspark.sql.functions import array, avg, col, count, explode, lit, split, timestamp_micros, window

from common import CHECKPOINT_DIR, GOLD_DIR, POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, build_spark


def write_batch(batch_df, _batch_id: int):
    if batch_df.isEmpty():
        return
    batch_df.write.mode("append").parquet(str(GOLD_DIR / "word_time_series"))
    batch_df.write.mode("append").jdbc(
        POSTGRES_JDBC_URL, "word_time_series", properties=POSTGRES_PROPERTIES
    )


def main():
    spark = build_spark("StructuredStreaming_WordTimeSeries")
    source_df = (
        spark.readStream.format("parquet")
        .load("D:/Bluesky-Reddit-BigDataProject/Bluesky_data/silver/vaderSentimentTable")
    )

    transformed = (
        source_df.filter(col("commit.record.langs") == array(lit("en")))
        .withColumn("timestamp", timestamp_micros(col("time_us")))
        .withColumn("word", explode(split(col("commit.record.text"), r"\s+")))
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

