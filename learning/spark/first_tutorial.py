from pyspark.sql import SparkSession
from delta import *

# 1. Configure the Spark builder to use Delta Lake
builder = SparkSession.builder.appName("Local_Lakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# 2. Inject the Delta Lake packages into your local Spark engine
spark = configure_spark_with_delta_pip(builder).getOrCreate()

df=spark.read.format('json').load("/home/harsh/big_data/final_project/Bluesky_data/initial_firehose/bluesky_2026-04-01_00.jsonl")
