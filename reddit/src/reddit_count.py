from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark Engine
# Starts the background Java engine for data processing
spark = SparkSession.builder.appName("RedditWordCount").getOrCreate()

print("Loading Data...")

# 2. Load the Bronze Data
# Reads all raw JSONL files into a Spark DataFrame
data_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\reddit_data\*.jsonl"
reddit_df = spark.read.json(data_path)

# 3. Set your Target Word
target_word = "Minecraft"

print(f"Searching for posts containing: {target_word}...")

# 4. Filter the DataFrame
# .filter(): Instructs Spark to only keep rows that match our condition.
# col("title").ilike(): Looks inside the 'title' column for our target word, ignoring uppercase/lowercase differences.
filtered_df = reddit_df.filter(col("title").ilike(f"%{target_word}%"))

# 5. Execute the Count
# .count(): This is a Spark "Action". It forces Spark to execute the filter across all files and return the final mathematical integer of how many rows survived.
post_count = filtered_df.count()

# 6. Output the Result
print("\n" + "="*40)
print(f"Total posts mentioning '{target_word}': {post_count}")
print("="*40 + "\n")

# Clean Shutdown
spark.stop()