import os

from pyspark.sql.functions import (
    col,
    count,
    get_json_object,
    lit,
    to_timestamp,
    window,
)

from common import (
    CHECKPOINT_DIR,
    GOLD_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_FIREHOSE_TOPIC,
    KAFKA_GETPOSTS_TOPIC,
    POSTGRES_JDBC_URL,
    POSTGRES_PROPERTIES,
    build_spark,
)

GETPOSTS_STREAM_DIR = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/streaming/getposts"
FIREHOSE_STREAM_DIR = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/streaming/firehose"
USE_KAFKA_SOURCES = os.getenv("USE_KAFKA_SOURCES", "false").lower() == "true"


def write_batch(batch_df, _batch_id: int):
    if batch_df.isEmpty():
        return
    batch_df.write.mode("append").parquet(str(GOLD_DIR / "ingestion_metrics_timeline"))
    batch_df.write.mode("append").jdbc(
        POSTGRES_JDBC_URL, "ingestion_metrics_timeline", properties=POSTGRES_PROPERTIES
    )


def main():
    spark = build_spark("StructuredStreaming_IngestionMetrics")

    if USE_KAFKA_SOURCES:
        firehose_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_FIREHOSE_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS raw_json")
            .withColumn(
                "event_time",
                to_timestamp(get_json_object(col("raw_json"), "$.time_us").cast("double") / lit(1_000_000)),
            )
            .withColumn("source_type", lit("firehose"))
        )

        getposts_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_GETPOSTS_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS raw_json")
            .withColumn("event_time", to_timestamp(get_json_object(col("raw_json"), "$.indexedAt")))
            .withColumn("source_type", lit("getPosts"))
        )
        checkpoint_subdir = "ingestion_metrics_timeline_kafka"
    else:
        firehose_stream = (
            spark.readStream.format("json")
            .load(FIREHOSE_STREAM_DIR)
            .withColumn("event_time", to_timestamp(col("time_us") / lit(1_000_000)))
            .withColumn("source_type", lit("firehose"))
        )

        getposts_stream = (
            spark.readStream.format("json")
            .load(GETPOSTS_STREAM_DIR)
            .withColumn("event_time", to_timestamp(col("indexedAt")))
            .withColumn("source_type", lit("getPosts"))
        )
        checkpoint_subdir = "ingestion_metrics_timeline"

    transformed = (
        firehose_stream.select("event_time", "source_type")
        .unionByName(getposts_stream.select("event_time", "source_type"))
        .filter(col("event_time").isNotNull())
        .groupBy(window(col("event_time"), "10 minutes").alias("time_window"), col("source_type"))
        .agg(count("*").alias("record_count"))
        .select(col("time_window.start").alias("time_bucket"), col("source_type"), col("record_count"))
    )

    query = (
        transformed.writeStream.outputMode("update")
        .option("checkpointLocation", str(CHECKPOINT_DIR / checkpoint_subdir))
        .foreachBatch(write_batch)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
