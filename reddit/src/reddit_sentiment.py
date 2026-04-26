from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 1. Initialize Spark Engine
# This starts the Big Data processing framework
spark = SparkSession.builder.appName("RedditSentiment").getOrCreate()

# 2. Initialize VADER
# This loads the NLP dictionary that understands social media sentiment
analyzer = SentimentIntensityAnalyzer()

# 3. Define the Scoring Logic
# This function takes a sentence and returns a score from -1.0 to +1.0
def get_sentiment_score(text):
    if text is None:
        return 0.0
    return analyzer.polarity_scores(text)['compound']

# 4. Create a Spark UDF (User Defined Function)
# This packages our Python function so Spark can run it across thousands of rows instantly
sentiment_udf = udf(get_sentiment_score, FloatType())

print("Loading Data...")

# 5. Load the Bronze Data
# This command reads all your raw JSONL files from the reddit_data folder at once
# UPDATE THIS PATH if your reddit_data folder is located somewhere else!
data_path = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\reddit_data\*.jsonl"
reddit_df = spark.read.json(data_path)

# 6. Set your Target
target_entity = "Minecraft"

print(f"Filtering and calculating sentiment for: {target_entity}...")

# 7. The Silver Layer Transformation
# This filters out irrelevant posts, throws away extra columns, and applies the sentiment score
reddit_scored = reddit_df.filter(col("title").ilike(f"%{target_entity}%")) \
                         .select(col("title").alias("text")) \
                         .withColumn("sentiment_score", sentiment_udf(col("text")))

# 8. The Gold Layer Aggregation
# This calculates the final average score of all matching posts combined
reddit_final = reddit_scored.select(avg("sentiment_score").alias("reddit_avg_sentiment"))

# 9. Output to Terminal
# This triggers the actual execution of the pipeline and prints the final number
reddit_final.show()

# 10. Clean Shutdown
spark.stop()