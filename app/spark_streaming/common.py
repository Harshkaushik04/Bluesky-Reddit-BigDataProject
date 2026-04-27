from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path("/mnt/d/Bluesky-Reddit-BigDataProject")
DATA_DIR = BASE_DIR / "Bluesky_data"
GOLD_DIR = DATA_DIR / "gold"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"

POSTGRES_JDBC_URL = "jdbc:postgresql://localhost:5432/bluesky_db"
POSTGRES_PROPERTIES = {
    "user": "backend_user",
    "password": "supersecretpassword",
    "driver": "org.postgresql.Driver",
}


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )