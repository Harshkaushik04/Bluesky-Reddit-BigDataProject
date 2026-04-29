import os

from pyspark.sql.functions import (
    array_distinct,
    col,
    coalesce,
    count,
    explode,
    get_json_object,
    lit,
    lower,
    split,
    to_timestamp,
    window,
)

from common import (
    CHECKPOINT_DIR,
    GOLD_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GETPOSTS_TOPIC,
    POSTGRES_JDBC_URL,
    POSTGRES_PROPERTIES,
    build_spark,
)

GETPOSTS_STREAM_DIR = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/streaming/getposts"
USE_KAFKA_GETPOSTS_SOURCE = os.getenv("USE_KAFKA_GETPOSTS_SOURCE", "false").lower() == "true"


def write_batch(batch_df, _batch_id: int):
    if batch_df.isEmpty():
        return
    batch_df.write.mode("append").parquet(str(GOLD_DIR / "reddit_crossover_stats"))
    batch_df.write.mode("append").jdbc(
        POSTGRES_JDBC_URL, "reddit_crossover_stats", properties=POSTGRES_PROPERTIES
    )


def main():
    spark = build_spark("StructuredStreaming_RedditCrossover")
    if USE_KAFKA_GETPOSTS_SOURCE:
        stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_GETPOSTS_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS raw_json")
            .withColumn("created_at", get_json_object(col("raw_json"), "$.record.createdAt"))
            .withColumn("record_text", get_json_object(col("raw_json"), "$.record.text"))
        )
        checkpoint_subdir = "reddit_crossover_stats_kafka"
        created_at_col = col("created_at")
        text_col = col("record_text")
    else:
        stream = spark.readStream.format("json").load(GETPOSTS_STREAM_DIR)
        checkpoint_subdir = "reddit_crossover_stats"
        created_at_col = col("record.createdAt")
        text_col = col("record.text")

    transformed = (
        stream.withColumn("timestamp", to_timestamp(created_at_col))
        .withColumn("text_clean", lower(coalesce(text_col, lit(""))))
        .filter(col("text_clean").contains("reddit.com"))
        .withColumn("topic_name", explode(array_distinct(split(col("text_clean"), r"\s+"))))
        .filter(~col("topic_name").contains("reddit.com"))
        .groupBy(window(col("timestamp"), "10 minutes").alias("time_window"), "topic_name")
        .agg(count("*").alias("reddit_link_count"))
        .select("topic_name", col("time_window.start").alias("time_bucket"), "reddit_link_count")
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