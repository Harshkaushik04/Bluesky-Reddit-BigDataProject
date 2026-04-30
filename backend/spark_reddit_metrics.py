"""
Spark Structured Streaming: watches posts_live/ and comments_live/ for new files,
processes each file exactly once, and appends rows to the SQLite database.
"""
from __future__ import annotations

import re
import sqlite3
import uuid
import threading
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, DoubleType,
)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "backend" / "reddit_dashboard.db"

POSTS_LIVE_DIR = str(BASE_DIR / "reddit_yash_ki_divya" / "data" / "posts_live")
COMMENTS_LIVE_DIR = str(BASE_DIR / "reddit_yash_ki_divya" / "data" / "comments_live")

POSTS_CHECKPOINT = str(BASE_DIR / "backend" / "checkpoint_posts")
COMMENTS_CHECKPOINT = str(BASE_DIR / "backend" / "checkpoint_comments")

# ---------- Schemas (only the fields we need) ----------
POSTS_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("subreddit", StringType()),
    StructField("title", StringType()),
    StructField("score", LongType()),
    StructField("ups", LongType()),
    StructField("downs", LongType()),
    StructField("num_comments", LongType()),
    StructField("post_hint", StringType()),
    StructField("is_video", BooleanType()),
    StructField("url", StringType()),
    StructField("domain", StringType()),
    StructField("url_overridden_by_dest", StringType()),
    StructField("selftext", StringType()),
    StructField("is_gallery", BooleanType()),
    StructField("crosspost_parent", StringType()),
    StructField("created_utc", DoubleType()),
])

COMMENTS_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("subreddit", StringType()),
    StructField("body", StringType()),
    StructField("score", LongType()),
    StructField("ups", LongType()),
    StructField("downs", LongType()),
    StructField("controversiality", LongType()),
    StructField("created_utc", DoubleType()),
])

# ---------- Post type classification ----------
_VIDEO_EXTS = re.compile(r"\.(mp4|mov|webm|mkv)(\?|$)")
_VIDEO_DOMAINS = re.compile(r"(v\.redd\.it|youtube\.com|youtu\.be|streamable\.com|twitch\.tv|redgifs\.com|gfycat\.com)")
_IMAGE_EXTS = re.compile(r"\.(jpg|jpeg|png|gif|webp)(\?|$)")
_IMAGE_DOMAINS = re.compile(r"(i\.redd\.it|imgur\.com|flickr\.com|images?)")

INSERT_SQL = (
    "INSERT INTO reddit_post_facts "
    "(run_id, row_uid, post_id, created_date, year, month, title, score, ups, downs, "
    "num_comments, engagement, post_type) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

COMMENT_INSERT_SQL = (
    "INSERT INTO reddit_comment_facts "
    "(run_id, row_uid, comment_id, created_date, year, month, body, score, ups, downs, "
    "controversiality) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

db_lock = threading.Lock()


def _get_run_id() -> int:
    """Get the latest run_id from the database (created by load_historic.py)."""
    with sqlite3.connect(str(DB_PATH)) as conn:
        row = conn.execute(
            "SELECT run_id FROM reddit_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
    if row:
        return row[0]
    raise RuntimeError("No run_id found in DB. Run load_historic.py first!")


def _classify_post_type(row, is_comment: bool) -> str:
    if is_comment:
        return "comment"
    post_hint = str(row["post_hint"] or "").lower() if row["post_hint"] else ""
    is_video = bool(row["is_video"]) if row["is_video"] is not None else False
    url = str(row["url"] or "").lower() if row["url"] else ""
    domain = str(row["domain"] or "").lower() if row["domain"] else ""
    url_dest = str(row["url_overridden_by_dest"] or "").lower() if row["url_overridden_by_dest"] else ""
    selftext = str(row["selftext"] or "").strip() if row["selftext"] else ""
    is_gallery = bool(row["is_gallery"]) if row["is_gallery"] is not None else False
    crosspost_parent = str(row["crosspost_parent"] or "") if row["crosspost_parent"] else ""
    if is_gallery:
        return "gallery"
    if is_video or post_hint in {"rich:video", "hosted:video", "video"} or _VIDEO_EXTS.search(url) or _VIDEO_EXTS.search(url_dest) or _VIDEO_DOMAINS.search(url) or _VIDEO_DOMAINS.search(url_dest) or _VIDEO_DOMAINS.search(domain):
        return "video"
    if post_hint == "image" or _IMAGE_EXTS.search(url) or _IMAGE_EXTS.search(url_dest) or _IMAGE_DOMAINS.search(domain):
        return "image"
    if post_hint == "poll":
        return "poll"
    if len(crosspost_parent) > 0:
        return "crosspost"
    if post_hint == "link" or (len(url) > 0 and len(selftext) == 0):
        return "link"
    if len(selftext) > 0:
        return "text"
    return "other"


def _process_posts_batch(df, batch_id):
    """Called by Spark for each micro-batch of new post files."""
    count = df.count()
    if count == 0:
        return
    run_id = _get_run_id()
    rows = df.collect()
    fact_rows = []
    for row in rows:
        created_utc = row["created_utc"]
        if created_utc is None:
            continue
        try:
            dt = datetime.utcfromtimestamp(float(created_utc))
        except (ValueError, TypeError, OSError):
            continue
        score = float(row["score"] or 0)
        ups = float(row["ups"] or 0)
        downs = float(row["downs"] or 0)
        num_comments = float(row["num_comments"] or 0)
        post_type = _classify_post_type(row, is_comment=False)
        fact_rows.append((
            run_id, str(uuid.uuid4()), str(row["id"] or ""),
            dt.strftime("%Y-%m-%d"), dt.year, dt.month,
            str(row["title"] or ""), score, ups, downs,
            num_comments, 1.0 + ups + downs + num_comments, post_type,
        ))
    if fact_rows:
        with db_lock:
            with sqlite3.connect(str(DB_PATH)) as conn:
                conn.executemany(INSERT_SQL, fact_rows)
                conn.execute(
                    "UPDATE reddit_runs SET records_scanned = records_scanned + ? WHERE run_id = ?",
                    (len(fact_rows), run_id),
                )
                conn.commit()
    print(f"[Posts  Batch {batch_id}] +{len(fact_rows)} rows (run_id={run_id})")


def _process_comments_batch(df, batch_id):
    """Called by Spark for each micro-batch of new comment files."""
    count = df.count()
    if count == 0:
        return
    run_id = _get_run_id()
    rows = df.collect()
    fact_rows = []
    for row in rows:
        created_utc = row["created_utc"]
        if created_utc is None:
            continue
        try:
            dt = datetime.utcfromtimestamp(float(created_utc))
        except (ValueError, TypeError, OSError):
            continue
        score = float(row["score"] or 0)
        ups = float(row["ups"] or 0)
        downs = float(row["downs"] or 0)
        controversiality = int(row["controversiality"] or 0)
        fact_rows.append((
            run_id, str(uuid.uuid4()), str(row["id"] or ""),
            dt.strftime("%Y-%m-%d"), dt.year, dt.month,
            str(row["body"] or ""), score, ups, downs,
            controversiality,
        ))
    if fact_rows:
        with db_lock:
            with sqlite3.connect(str(DB_PATH)) as conn:
                conn.executemany(COMMENT_INSERT_SQL, fact_rows)
                conn.execute(
                    "UPDATE reddit_runs SET records_scanned = records_scanned + ? WHERE run_id = ?",
                    (len(fact_rows), run_id),
                )
                conn.commit()
    print(f"[Comts Batch {batch_id}] +{len(fact_rows)} rows (run_id={run_id})")


def main():
    run_id = _get_run_id()
    print(f"Using run_id={run_id} from database")

    spark = (
        SparkSession.builder
        .appName("RedditStructuredStreaming")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Posts stream ---
    posts_stream = (
        spark.readStream
        .format("json")
        .schema(POSTS_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .option("cleanSource", "off")
        .load(POSTS_LIVE_DIR)
    )
    posts_query = (
        posts_stream.writeStream
        .foreachBatch(_process_posts_batch)
        .option("checkpointLocation", POSTS_CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"Posts stream started — watching {POSTS_LIVE_DIR}")

    # --- Comments stream ---
    comments_stream = (
        spark.readStream
        .format("json")
        .schema(COMMENTS_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .option("cleanSource", "off")
        .load(COMMENTS_LIVE_DIR)
    )
    comments_query = (
        comments_stream.writeStream
        .foreachBatch(_process_comments_batch)
        .option("checkpointLocation", COMMENTS_CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"Comments stream started — watching {COMMENTS_LIVE_DIR}")

    print("\nSpark Structured Streaming is running. Press Ctrl+C to stop.")
    print("New files in posts_live/ and comments_live/ will be auto-detected.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping streams...")
        posts_query.stop()
        comments_query.stop()
        spark.stop()
        print("Done.")


if __name__ == "__main__":
    main()
