from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace, lower, udf, desc
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
import itertools

# 1. Initialize Spark Engine
spark = SparkSession.builder \
    .appName("CommunityGraphEdges") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 2. Define the Graph Schema
# This tells Spark we are going to create an array of "Source -> Target" links
schema = ArrayType(StructType([
    StructField("source", StringType(), False),
    StructField("target", StringType(), False)
]))

# 3. Create the Edge Generator (Python UDF)
# This looks at a Reddit title and links every word to every other word in that title
def get_edges(words):
    if not words:
        return []
    # Filter out small words ("the", "and") to keep the graph focused on real topics
    valid_words = [w for w in words if len(w) > 3]
    if len(valid_words) < 2:
        return []
        
    # Sort alphabetically to ensure A->B and B->A are counted as the exact same connection
    unique_words = sorted(list(set(valid_words)))
    
    # Generate all possible pairs of words in the sentence
    pairs = list(itertools.combinations(unique_words, 2))
    
    return [{"source": p[0], "target": p[1]} for p in pairs]

edges_udf = udf(get_edges, schema)

print("Loading Bronze Data...")
data_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\reddit_data\*.jsonl"
reddit_df = spark.read.json(data_path)

print("Extracting Network Edges...")
# 4. Clean and Split
clean_text = regexp_replace(lower(col("title")), r"[^a-z\s]", "")
words_array = split(clean_text, r"\s+")

# 5. Apply the Edge Generator
edges_df = reddit_df.withColumn("edges", edges_udf(words_array))

# 6. Explode into Rows
# Turns the array of links into individual rows of data
exploded_edges = edges_df.select(explode(col("edges")).alias("edge"))

# 7. Extract the exact Source and Target columns
final_edges = exploded_edges.select(
    col("edge.source").alias("source"),
    col("edge.target").alias("target")
)

print("Calculating Graph Community Weights...")
# 8. The MapReduce Aggregation
# Group by the pair of words and count how many times they appeared together.
# We filter out any weight less than 5 to remove random typos and keep the Graph DB fast!
weighted_graph = final_edges.groupBy("source", "target") \
                            .count() \
                            .withColumnRenamed("count", "weight") \
                            .filter(col("weight") > 5) \
                            .orderBy(desc("weight"))

print("Writing Edge List to Output Folder...")
# 9. Save the Gold Edge Data
output_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\output\graph_edges"
weighted_graph.coalesce(1).write.mode("overwrite").json(output_path)

print(f"Success! Graph Database Edge List saved to: {output_path}")

# Quick peek at your strongest communities
print("\n--- 🕸️ Strongest Community Links ---")
weighted_graph.show(10)

spark.stop()