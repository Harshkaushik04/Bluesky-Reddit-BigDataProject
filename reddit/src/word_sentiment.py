from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, split, regexp_replace, lower
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 1. Initialize Spark Engine
spark = SparkSession.builder.appName("WordLevelSentiment").getOrCreate()
analyzer = SentimentIntensityAnalyzer()

# 2. Define the Scoring Logic
def get_sentiment_score(text):
    if text is None:
        return 0.0
    return analyzer.polarity_scores(text)['compound']

sentiment_udf = udf(get_sentiment_score, FloatType())

print("Loading Data...")

# 3. Load the Bronze Data (Update this path if necessary)
data_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\reddit_data\*.jsonl"
reddit_df = spark.read.json(data_path)

print("Cleaning and Extracting Words...")

# 4. The "Tokenization" Layer
# Step A: Convert everything to lowercase and remove all punctuation (commas, periods, etc.)
clean_text = regexp_replace(lower(col("title")), r"[^a-z\s]", "")

# Step B: Split the clean sentence into a Python Array of words (e.g., ["i", "love", "minecraft"])
words_array = split(clean_text, r"\s+")

# 5. The "Explode" Layer
# .explode() takes an array and creates a brand new row for every single item in the array
# .distinct() ensures we only score each word once, saving massive amounts of processing time
unique_words_df = reddit_df.select(explode(words_array).alias("word")) \
                           .filter(col("word") != "") \
                           .distinct()

print("Calculating Sentiment for every unique word...")

# 6. Apply the Score
scored_words = unique_words_df.withColumn("sentiment_score", sentiment_udf(col("word")))

# Optional: Filter out neutral words (0.0). 
# VADER scores words like "the", "and", "it" as 0.0. 
# Uncomment the line below if you ONLY want positive/negative words in your JSON.
# scored_words = scored_words.filter(col("sentiment_score") != 0.0)

print("Writing to JSON file...")

# 7. Write the Gold Data to a JSON file
# coalesce(1) forces Spark to combine everything into ONE single file instead of hundreds of tiny files
output_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\output\word_scores"

scored_words.coalesce(1).write.mode("overwrite").json(output_path)

print(f"Success! File saved to: {output_path}")

spark.stop()