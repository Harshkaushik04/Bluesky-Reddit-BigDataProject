from pyspark.sql import SparkSession
from delta import *

# 1. Configure the Spark builder to use Delta Lake
builder = SparkSession.builder.appName("Local_Lakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# 2. Inject the Delta Lake packages into your local Spark engine
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Now you can write the exact same code from the tutorial!
# df = spark.readStream.format("kafka")...