from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE_DIR = Path(__file__).resolve().parent.parent
POSTS_GLOB = str(BASE_DIR / "reddit_yash_ki_divya" / "data" / "posts" / "*.jsonl")
DB_PATH = BASE_DIR / "backend" / "reddit_dashboard.db"
TOP_K = 6
TIMELINE_K = 12

STOPWORDS = [
    "about",
    "after",
    "all",
    "also",
    "and",
    "any",
    "are",
    "been",
    "before",
    "being",
    "but",
    "can",
    "could",
    "did",
    "does",
    "for",
    "from",
    "had",
    "has",
    "have",
    "her",
    "here",
    "him",
    "his",
    "how",
    "its",
    "just",
    "more",
    "most",
    "not",
    "now",
    "our",
    "out",
    "she",
    "that",
    "the",
    "their",
    "them",
    "there",
    "they",
    "this",
    "too",
    "was",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "will",
    "with",
    "would",
    "you",
    "your",
]


def ensure_post_fact_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(reddit_post_facts)").fetchall()
    }
    if "post_type" not in existing_columns:
        conn.execute("ALTER TABLE reddit_post_facts ADD COLUMN post_type TEXT")


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS reddit_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_ts TEXT NOT NULL,
            source_path TEXT NOT NULL,
            records_scanned INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS reddit_kpis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            total_content INTEGER NOT NULL,
            total_posts INTEGER NOT NULL,
            avg_sentiment REAL NOT NULL,
            avg_score REAL NOT NULL,
            avg_comments REAL NOT NULL,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS reddit_content_split (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            posts INTEGER NOT NULL,
            comments INTEGER NOT NULL,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS reddit_top_subreddits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            label TEXT NOT NULL,
            value INTEGER NOT NULL,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS reddit_top_keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            label TEXT NOT NULL,
            value INTEGER NOT NULL,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS reddit_timeline (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            bucket TEXT NOT NULL,
            count INTEGER NOT NULL,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS reddit_post_facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            row_uid TEXT NOT NULL,
            post_id TEXT,
            created_date TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            title TEXT,
            score REAL NOT NULL,
            ups REAL NOT NULL,
            downs REAL NOT NULL,
            num_comments REAL NOT NULL,
            engagement REAL NOT NULL,
            post_type TEXT,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );
        """
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("RedditMetricsBatch")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    posts_df = spark.read.json(POSTS_GLOB).select(
        F.col("id").alias("post_id"),
        F.col("subreddit"),
        F.col("title"),
        F.col("score").cast("double").alias("score"),
        F.col("ups").cast("double").alias("ups"),
        F.col("downs").cast("double").alias("downs"),
        F.col("num_comments").cast("double").alias("num_comments"),
        F.col("post_hint"),
        F.col("is_video"),
        F.col("url"),
        F.col("selftext"),
        F.col("created_utc").cast("double").alias("created_utc"),
    )

    records_scanned = posts_df.count()
    avg_row = posts_df.select(
        F.coalesce(F.avg("score"), F.lit(0.0)).alias("avg_score"),
        F.coalesce(F.avg("num_comments"), F.lit(0.0)).alias("avg_comments"),
    ).first()
    avg_score = float(avg_row["avg_score"])
    avg_comments = float(avg_row["avg_comments"])

    top_subreddits = (
        posts_df.groupBy("subreddit")
        .count()
        .where(F.col("subreddit").isNotNull() & (F.length("subreddit") > 0))
        .orderBy(F.col("count").desc())
        .limit(TOP_K)
        .collect()
    )

    words_df = (
        posts_df.withColumn("title_clean", F.lower(F.coalesce(F.col("title"), F.lit(""))))
        .withColumn("title_clean", F.regexp_replace("title_clean", r"[^a-zA-Z\s]", " "))
        .withColumn("word", F.explode(F.split(F.col("title_clean"), r"\s+")))
        .where((F.length("word") >= 3) & (~F.col("word").isin(STOPWORDS)))
    )

    top_keywords = (
        words_df.groupBy("word")
        .count()
        .orderBy(F.col("count").desc())
        .limit(TOP_K)
        .collect()
    )

    timeline = (
        posts_df.withColumn(
            "bucket", F.date_format(F.from_unixtime(F.col("created_utc")), "yyyy-MM-dd")
        )
        .groupBy("bucket")
        .count()
        .where(F.col("bucket").isNotNull())
        .orderBy(F.col("bucket").desc())
        .limit(TIMELINE_K)
        .orderBy(F.col("bucket").asc())
        .collect()
    )

    run_ts = datetime.now(timezone.utc).isoformat()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        create_tables(conn)
        ensure_post_fact_columns(conn)
        cursor = conn.execute(
            """
            INSERT INTO reddit_runs (run_ts, source_path, records_scanned)
            VALUES (?, ?, ?)
            """,
            (run_ts, POSTS_GLOB, records_scanned),
        )
        run_id = cursor.lastrowid

        conn.execute(
            """
            INSERT INTO reddit_kpis
            (run_id, total_content, total_posts, avg_sentiment, avg_score, avg_comments)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, records_scanned, records_scanned, 0.0, round(avg_score, 2), round(avg_comments, 2)),
        )
        conn.execute(
            """
            INSERT INTO reddit_content_split (run_id, posts, comments)
            VALUES (?, ?, ?)
            """,
            (run_id, records_scanned, 0),
        )

        conn.executemany(
            """
            INSERT INTO reddit_top_subreddits (run_id, rank, label, value)
            VALUES (?, ?, ?, ?)
            """,
            [
                (run_id, idx + 1, row["subreddit"], int(row["count"]))
                for idx, row in enumerate(top_subreddits)
            ],
        )
        conn.executemany(
            """
            INSERT INTO reddit_top_keywords (run_id, rank, label, value)
            VALUES (?, ?, ?, ?)
            """,
            [
                (run_id, idx + 1, row["word"], int(row["count"]))
                for idx, row in enumerate(top_keywords)
            ],
        )
        conn.executemany(
            """
            INSERT INTO reddit_timeline (run_id, bucket, count)
            VALUES (?, ?, ?)
            """,
            [(run_id, row["bucket"], int(row["count"])) for row in timeline],
        )

        facts_rows = (
            posts_df.withColumn(
                "created_date",
                F.date_format(F.from_unixtime(F.col("created_utc")), "yyyy-MM-dd"),
            )
            .withColumn("year", F.year(F.to_date(F.col("created_date"))))
            .withColumn("month", F.month(F.to_date(F.col("created_date"))))
            .withColumn("score", F.coalesce(F.col("score"), F.lit(0.0)))
            .withColumn("ups", F.coalesce(F.col("ups"), F.lit(0.0)))
            .withColumn("downs", F.coalesce(F.col("downs"), F.lit(0.0)))
            .withColumn("num_comments", F.coalesce(F.col("num_comments"), F.lit(0.0)))
            .withColumn("post_hint", F.lower(F.coalesce(F.col("post_hint"), F.lit(""))))
            .withColumn("is_video", F.coalesce(F.col("is_video"), F.lit(False)))
            .withColumn("url", F.lower(F.coalesce(F.col("url"), F.lit(""))))
            .withColumn("selftext", F.trim(F.coalesce(F.col("selftext"), F.lit(""))))
            .withColumn(
                "post_type",
                F.when(F.col("is_video") == True, F.lit("video"))
                .when(
                    F.col("post_hint").isin("image", "rich:video", "hosted:video")
                    | F.col("url").rlike(r"\\.(jpg|jpeg|png|gif|webp)(\\?|$)"),
                    F.lit("photo"),
                )
                .when(F.length(F.col("selftext")) > 0, F.lit("text"))
                .otherwise(F.lit("other")),
            )
            .withColumn(
                "engagement",
                F.lit(1.0) + F.col("ups") + F.col("downs") + F.col("num_comments"),
            )
            .select(
                "post_id",
                "created_date",
                "year",
                "month",
                "title",
                "score",
                "ups",
                "downs",
                "num_comments",
                "engagement",
                "post_type",
            )
            .where(F.col("created_date").isNotNull())
            .collect()
        )

        conn.executemany(
            """
            INSERT INTO reddit_post_facts
            (run_id, row_uid, post_id, created_date, year, month, title, score, ups, downs, num_comments, engagement, post_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    str(uuid.uuid4()),
                    row["post_id"],
                    row["created_date"],
                    int(row["year"]),
                    int(row["month"]),
                    row["title"],
                    float(row["score"]),
                    float(row["ups"]),
                    float(row["downs"]),
                    float(row["num_comments"]),
                    float(row["engagement"]),
                    str(row["post_type"]),
                )
                for row in facts_rows
            ],
        )
        conn.commit()

    spark.stop()
    print(f"Run complete. run_id={run_id}, records_scanned={records_scanned}, db={DB_PATH}")


if __name__ == "__main__":
    main()
