import os
from pathlib import Path
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load the central .env from the project root
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

BASE_DIR = Path(os.getenv("PROJECT_BASE_DIR", Path(__file__).resolve().parent.parent.parent))
DATA_DIR = Path(os.getenv("BLUESKY_DATA_DIR", BASE_DIR / "Bluesky_data")).resolve()

FIREHOSE_STREAM_DIR = DATA_DIR / "streaming" / "firehose"
GETPOSTS_STREAM_DIR = DATA_DIR / "streaming" / "getposts"
GOLD_DIR = DATA_DIR / "gold"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_FIREHOSE_TOPIC = os.getenv("KAFKA_FIREHOSE_TOPIC", "bluesky.firehose.raw")
KAFKA_GETPOSTS_TOPIC = os.getenv("KAFKA_GETPOSTS_TOPIC", "bluesky.getposts.raw")

POSTGRES_JDBC_URL = "jdbc:postgresql://localhost:5432/bluesky_db"
POSTGRES_PROPERTIES = {
    "user": "backend_user",
    "password": "supersecretpassword",
    "driver": "org.postgresql.Driver",
}


def build_spark(app_name: str) -> SparkSession:
    spark_packages = "org.postgresql:postgresql:42.6.0"
    if os.getenv("SPARK_KAFKA_ENABLED", "false").lower() == "true":
        spark_packages += ",org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.jars.packages", spark_packages)
        .getOrCreate()
    )