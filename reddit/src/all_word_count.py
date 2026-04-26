from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace, lower, length, desc

# 1. Initialize Spark Engine (Keeping the 4GB RAM upgrade!)
spark = SparkSession.builder \
    .appName("GlobalWordCount") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("Loading Bronze Data...")

# 2. Load the Raw Data
data_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\reddit_data\*.jsonl"
reddit_df = spark.read.json(data_path)

print("Cleaning and Tokenizing Text...")

# 3. The Tokenization Layer
# Convert to lowercase and strip out all punctuation
clean_text = regexp_replace(lower(col("title")), r"[^a-z\s]", "")
# Split the sentence into an array of words
words_array = split(clean_text, r"\s+")

# 4. The Explode Layer
# Turn the array into individual rows (e.g., one row becomes 15 rows if there are 15 words)
exploded_df = reddit_df.select(explode(words_array).alias("word"))

print("Aggregating Global Word Counts...")

# 5. The MapReduce Layer (Grouping and Counting)
# We filter out words less than 4 characters (like "the", "and", "a") to keep the data meaningful
word_counts_df = exploded_df.filter(length(col("word")) > 3) \
                            .groupBy("word") \
                            .count() \
                            .withColumnRenamed("count", "total_occurrences")

# 6. Sort the Data
# Order from the most used word to the least used word
sorted_word_counts = word_counts_df.orderBy(desc("total_occurrences"))

print("Writing to Output Folder...")

# 7. Write the Gold Data to the new Output directory
output_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\output\global_word_count"
sorted_word_counts.coalesce(1).write.mode("overwrite").json(output_path)

print(f"Success! Global Word Count saved to: {output_path}")

# 8. Instant Verification
print("\n--- 🏆 Top 10 Most Used Words in Your Dataset ---")
sorted_word_counts.show(10)

# Clean Shutdown
spark.stop()