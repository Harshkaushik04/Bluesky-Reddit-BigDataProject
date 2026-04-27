from pyspark.sql.functions import array_distinct, col, coalesce, count, explode, lower, split, to_timestamp, window

from common import CHECKPOINT_DIR, GOLD_DIR, POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, build_spark


def write_batch(batch_df, _batch_id: int):
    if batch_df.isEmpty():
        return
    batch_df.write.mode("append").parquet(str(GOLD_DIR / "reddit_crossover_stats"))
    batch_df.write.mode("append").jdbc(
        POSTGRES_JDBC_URL, "reddit_crossover_stats", properties=POSTGRES_PROPERTIES
    )


def main():
    spark = build_spark("StructuredStreaming_RedditCrossover")
    stream = spark.readStream.format("json").load(
        "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/streaming/getposts"
    )

    transformed = (
        stream.withColumn("timestamp", to_timestamp(col("record.createdAt")))
        .filter(lower(coalesce(col("record.text"), lit(""))).contains("reddit.com"))
        .withColumn("topic_name", explode(array_distinct(split(lower(coalesce(col("record.text"), lit(""))), r"\s+"))))
        .filter(~col("topic_name").contains("reddit.com"))
        .groupBy(window(col("timestamp"), "10 minutes").alias("time_window"), "topic_name")
        .agg(count("*").alias("reddit_link_count"))
        .select("topic_name", col("time_window.start").alias("time_bucket"), "reddit_link_count")
    )

    query = (
        transformed.writeStream.outputMode("update")
        .option("checkpointLocation", str(CHECKPOINT_DIR / "reddit_crossover_stats"))
        .foreachBatch(write_batch)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()