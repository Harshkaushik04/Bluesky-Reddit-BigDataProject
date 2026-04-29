import os

from pyspark.sql.functions import (
    array_contains,
    avg,
    coalesce,
    col,
    count,
    explode,
    get_json_object,
    lit,
    split,
    timestamp_micros,
    udf,
    window,
)
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from common import (
    CHECKPOINT_DIR,
    FIREHOSE_STREAM_DIR,
    GOLD_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_FIREHOSE_TOPIC,
    POSTGRES_JDBC_URL,
    POSTGRES_PROPERTIES,
    build_spark,
)

CHECKPOINT_SUBDIR = "word_time_series_firehose_streaming"
SENTIMENT_GOLD_SUBDIR = "vaderSentimentAnalysisFinal"
USE_KAFKA_SOURCE = os.getenv("USE_KAFKA_FIREHOSE_SOURCE", "false").lower() == "true"

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
    batch_df.write.mode("append").parquet(str(GOLD_DIR / SENTIMENT_GOLD_SUBDIR))
    batch_df.write.mode("append").jdbc(
        POSTGRES_JDBC_URL, "word_time_series", properties=POSTGRES_PROPERTIES
    )


def main():
    spark = build_spark("StructuredStreaming_WordTimeSeries")

    if USE_KAFKA_SOURCE:
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_FIREHOSE_TOPIC)
            .option("startingOffsets", "latest")
            .load()
        )
        # Keep extraction lightweight by selecting only the JSON fields we use downstream.
        source_df = kafka_df.selectExpr("CAST(value AS STRING) AS raw_json").select(
            get_json_object(col("raw_json"), "$.commit.record.langs").alias("langs_raw"),
            get_json_object(col("raw_json"), "$.commit.record.text").alias("text_raw"),
            get_json_object(col("raw_json"), "$.time_us").cast("long").alias("time_us"),
        ).withColumn(
            "commit",
            lit(None),
        ).withColumn(
            "commit_record_text",
            col("text_raw"),
        ).withColumn(
            "commit_record_langs",
            col("langs_raw"),
        )
        prepared_df = (
            source_df.withColumn("text_clean", coalesce(col("commit_record_text"), lit("")))
            .withColumn("timestamp", timestamp_micros(col("time_us")))
            .filter(col("text_clean") != "")
            .filter(col("commit_record_langs").contains("en"))
        )
    else:
        # Read raw JSON records directly from the firehose streaming output directory.
        source_df = spark.readStream.format("json").load(str(FIREHOSE_STREAM_DIR))
        prepared_df = (
            source_df.filter(array_contains(col("commit.record.langs"), "en"))
            .withColumn("timestamp", timestamp_micros(col("time_us")))
            .withColumn("text_clean", coalesce(col("commit.record.text"), lit("")))
            .filter(col("text_clean") != "")
        )

    transformed = (
        prepared_df
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
        .option("checkpointLocation", str(CHECKPOINT_DIR / CHECKPOINT_SUBDIR))
        .foreachBatch(write_batch)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()