CREATE TABLE IF NOT EXISTS word_time_series (
    word VARCHAR(255),
    time_range TIMESTAMP,
    avg_vader_sentiment_score FLOAT,
    word_count INT
);

CREATE TABLE IF NOT EXISTS ingestion_metrics_timeline (
    time_bucket TIMESTAMP,
    source_type VARCHAR(50),
    record_count INT
);

CREATE TABLE IF NOT EXISTS controversial_topics_timeline (
    topic_name VARCHAR(255),
    time_bucket TIMESTAMP,
    average_like_to_comment_ratio FLOAT
);

CREATE TABLE IF NOT EXISTS reddit_crossover_stats (
    topic_name VARCHAR(255),
    time_bucket TIMESTAMP,
    reddit_link_count INT
);

