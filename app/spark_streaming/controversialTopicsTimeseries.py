import os

from pyspark.sql.functions import (
    array_distinct,
    avg,
    col,
    coalesce,
    explode,
    get_json_object,
    lit,
    lower,
    split,
    to_timestamp,
    window,
    when,
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
    batch_df.write.mode("append").parquet(str(GOLD_DIR / "controversial_topics_timeline"))
    batch_df.write.mode("append").jdbc(
        POSTGRES_JDBC_URL, "controversial_topics_timeline", properties=POSTGRES_PROPERTIES
    )

def main():
    spark = build_spark("StructuredStreaming_ControversialTopics")

    if USE_KAFKA_GETPOSTS_SOURCE:
        stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_GETPOSTS_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS raw_json")
            .withColumn("langs_raw", get_json_object(col("raw_json"), "$.record.langs"))
            .withColumn("replyCount", get_json_object(col("raw_json"), "$.replyCount").cast("double"))
            .withColumn("likeCount", get_json_object(col("raw_json"), "$.likeCount").cast("double"))
            .withColumn("indexedAt", get_json_object(col("raw_json"), "$.indexedAt"))
            .withColumn("record_text", get_json_object(col("raw_json"), "$.record.text"))
        )
        checkpoint_subdir = "controversial_topics_timeline_kafka"
        lang_filter = col("langs_raw").contains("\"en\"")
        text_col = col("record_text")
    else:
        stream = spark.readStream.format("json").load(GETPOSTS_STREAM_DIR)
        checkpoint_subdir = "controversial_topics_timeline"
        lang_filter = col("record.langs").contains("en")
        text_col = col("record.text")

    transformed = (
        stream.filter(lang_filter)
        .withColumn(
            "like_to_comment_ratio",
            when(col("replyCount") == 0, lit(0.0)).otherwise(col("likeCount") / col("replyCount")),
        )
        .filter((col("like_to_comment_ratio") != 0) & (col("replyCount") >= 50))
        .withColumn("timestamp", to_timestamp(col("indexedAt")))
        # Create a clean text column before exploding to save computation
        .withColumn("text_clean", lower(coalesce(text_col, lit(""))))
        .filter(col("text_clean") != "")
        .withColumn("topic_name", explode(array_distinct(split(col("text_clean"), r"\s+"))))
        .filter(col("topic_name") != "")
        .groupBy(window(col("timestamp"), "10 minutes").alias("time_window"), "topic_name")
        .agg(avg(col("like_to_comment_ratio")).alias("average_like_to_comment_ratio"))
        .select(
            col("time_window.start").alias("time_bucket"),
            col("topic_name"),
            col("average_like_to_comment_ratio"),
        )
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