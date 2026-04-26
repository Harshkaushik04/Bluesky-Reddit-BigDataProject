from pyspark.sql.functions import (
    array,
    array_distinct,
    avg,
    col,
    explode,
    lit,
    lower,
    split,
    to_timestamp,
    window,
    when,
)

from common import CHECKPOINT_DIR, GOLD_DIR, POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, build_spark


def write_batch(batch_df, _batch_id: int):
    if batch_df.isEmpty():
        return
    batch_df.write.mode("append").parquet(str(GOLD_DIR / "controversial_topics_timeline"))
    batch_df.write.mode("append").jdbc(
        POSTGRES_JDBC_URL, "controversial_topics_timeline", properties=POSTGRES_PROPERTIES
    )


def main():
    spark = build_spark("StructuredStreaming_ControversialTopics")
    stream = spark.readStream.format("json").load(
        "D:/Bluesky-Reddit-BigDataProject/Bluesky_data/streaming/getposts"
    )

    transformed = (
        stream.filter(col("record.langs") == array(lit("en")))
        .withColumn(
            "like_to_comment_ratio",
            when(col("replyCount") == 0, lit(0.0)).otherwise(col("likeCount") / col("replyCount")),
        )
        .filter((col("like_to_comment_ratio") != 0) & (col("replyCount") >= 50))
        .withColumn("timestamp", to_timestamp(col("indexedAt")))
        .withColumn("topic_name", explode(array_distinct(split(lower(col("record.text")), r"\s+"))))
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
        .option("checkpointLocation", str(CHECKPOINT_DIR / "controversial_topics_timeline"))
        .foreachBatch(write_batch)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

