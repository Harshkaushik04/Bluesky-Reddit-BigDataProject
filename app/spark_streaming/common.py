from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path("D:/Bluesky-Reddit-BigDataProject")
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
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )

