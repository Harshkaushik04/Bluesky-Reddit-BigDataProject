from pyspark.sql.functions import col, lit, to_timestamp, window

from common import CHECKPOINT_DIR, GOLD_DIR, POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, build_spark


def write_batch(batch_df, _batch_id: int):
    if batch_df.isEmpty():
        return
    batch_df.write.mode("append").parquet(str(GOLD_DIR / "ingestion_metrics_timeline"))
    batch_df.write.mode("append").jdbc(
        POSTGRES_JDBC_URL, "ingestion_metrics_timeline", properties=POSTGRES_PROPERTIES
    )


def main():
    spark = build_spark("StructuredStreaming_IngestionMetrics")

    firehose_stream = (
        spark.readStream.format("json")
        .load("D:/Bluesky-Reddit-BigDataProject/Bluesky_data/streaming/firehose")
        .withColumn("timestamp", to_timestamp(col("commit.record.createdAt")))
        .groupBy(window(col("timestamp"), "2 hours").alias("time_window"))
        .count()
        .withColumn("source_type", lit("firehose"))
        .select(col("time_window.start").alias("time_bucket"), "source_type", col("count").alias("record_count"))
    )

    getposts_stream = (
        spark.readStream.format("json")
        .load("D:/Bluesky-Reddit-BigDataProject/Bluesky_data/streaming/getposts")
        .withColumn("timestamp", to_timestamp(col("record.createdAt")))
        .groupBy(window(col("timestamp"), "2 hours").alias("time_window"))
        .count()
        .withColumn("source_type", lit("getPosts_endpoint"))
        .select(col("time_window.start").alias("time_bucket"), "source_type", col("count").alias("record_count"))
    )

    unified = firehose_stream.unionByName(getposts_stream, allowMissingColumns=True)

    query = (
        unified.writeStream.outputMode("update")
        .option("checkpointLocation", str(CHECKPOINT_DIR / "ingestion_metrics_timeline"))
        .foreachBatch(write_batch)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

