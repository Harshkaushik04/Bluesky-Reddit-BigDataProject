"""
One-time script: load all historic data (posts/ and comments/) into a fresh DB.
Spark Structured Streaming will handle posts_live/ and comments_live/ separately.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "reddit_yash_ki_divya" / "data"
DB_PATH = BASE_DIR / "backend" / "reddit_dashboard.db"

# Directories to load (historic only — Spark streaming handles live dirs)
DIRS = [
    (DATA_DIR / "posts", False),       # is_comment=False
    (DATA_DIR / "comments", True),     # is_comment=True
]

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

_VIDEO_EXTS = re.compile(r"\.(mp4|mov|webm|mkv)(\?|$)")
_VIDEO_DOMAINS = re.compile(r"(v\.redd\.it|youtube\.com|youtu\.be|streamable\.com|twitch\.tv|redgifs\.com|gfycat\.com)")
_IMAGE_EXTS = re.compile(r"\.(jpg|jpeg|png|gif|webp)(\?|$)")
_IMAGE_DOMAINS = re.compile(r"(i\.redd\.it|imgur\.com|flickr\.com|images?)")


def classify_post_type(record: dict, is_comment: bool) -> str:
    if is_comment:
        return "comment"
    post_hint = str(record.get("post_hint", "") or "").lower()
    is_video = bool(record.get("is_video", False))
    url = str(record.get("url", "") or "").lower()
    domain = str(record.get("domain", "") or "").lower()
    url_dest = str(record.get("url_overridden_by_dest", "") or "").lower()
    selftext = str(record.get("selftext", "") or "").strip()
    is_gallery = bool(record.get("is_gallery", False))
    crosspost_parent = str(record.get("crosspost_parent", "") or "")
    poll_data = record.get("poll_data")
    if is_gallery:
        return "gallery"
    if is_video or post_hint in {"rich:video", "hosted:video", "video"} or _VIDEO_EXTS.search(url) or _VIDEO_EXTS.search(url_dest) or _VIDEO_DOMAINS.search(url) or _VIDEO_DOMAINS.search(url_dest) or _VIDEO_DOMAINS.search(domain):
        return "video"
    if post_hint == "image" or _IMAGE_EXTS.search(url) or _IMAGE_EXTS.search(url_dest) or _IMAGE_DOMAINS.search(domain):
        return "image"
    if poll_data is not None or post_hint == "poll":
        return "poll"
    if len(crosspost_parent) > 0:
        return "crosspost"
    if post_hint == "link" or (len(url) > 0 and len(selftext) == 0):
        return "link"
    if len(selftext) > 0:
        return "text"
    return "other"


def parse_fact_row(record: dict, run_id: int, is_comment: bool):
    created_utc = record.get("created_utc")
    if created_utc is None:
        return None
    try:
        dt = datetime.utcfromtimestamp(float(created_utc))
    except (ValueError, TypeError, OSError):
        return None
    post_id = str(record.get("id", ""))
    if is_comment:
        title = str(record.get("body", "") or "")
        num_comments = 0.0
    else:
        title = str(record.get("title", "") or "")
        num_comments = float(record.get("num_comments", 0) or 0)
    score = float(record.get("score", 0) or 0)
    ups = float(record.get("ups", 0) or 0)
    downs = float(record.get("downs", 0) or 0)
    engagement = 1.0 + ups + downs + num_comments
    post_type = classify_post_type(record, is_comment)
    return (
        run_id, str(uuid.uuid4()), post_id,
        dt.strftime("%Y-%m-%d"), dt.year, dt.month,
        title, score, ups, downs, num_comments, engagement, post_type,
    )


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS reddit_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_ts TEXT NOT NULL,
            source_path TEXT NOT NULL,
            records_scanned INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reddit_kpis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            total_content INTEGER NOT NULL, total_posts INTEGER NOT NULL,
            avg_sentiment REAL NOT NULL, avg_score REAL NOT NULL, avg_comments REAL NOT NULL,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS reddit_content_split (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL, posts INTEGER NOT NULL, comments INTEGER NOT NULL,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS reddit_top_subreddits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL, rank INTEGER NOT NULL, label TEXT NOT NULL, value INTEGER NOT NULL,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS reddit_top_keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL, rank INTEGER NOT NULL, label TEXT NOT NULL, value INTEGER NOT NULL,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS reddit_timeline (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL, bucket TEXT NOT NULL, count INTEGER NOT NULL,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS reddit_post_facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL, row_uid TEXT NOT NULL, post_id TEXT,
            created_date TEXT NOT NULL, year INTEGER NOT NULL, month INTEGER NOT NULL,
            title TEXT, score REAL NOT NULL, ups REAL NOT NULL, downs REAL NOT NULL,
            num_comments REAL NOT NULL, engagement REAL NOT NULL, post_type TEXT,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS reddit_comment_facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL, row_uid TEXT NOT NULL, comment_id TEXT,
            created_date TEXT NOT NULL, year INTEGER NOT NULL, month INTEGER NOT NULL,
            body TEXT, score REAL NOT NULL, ups REAL NOT NULL, downs REAL NOT NULL,
            controversiality INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (run_id) REFERENCES reddit_runs(run_id)
        );
    """)


def parse_comment_fact_row(record: dict, run_id: int):
    created_utc = record.get("created_utc")
    if created_utc is None:
        return None
    try:
        dt = datetime.utcfromtimestamp(float(created_utc))
    except (ValueError, TypeError, OSError):
        return None
    comment_id = str(record.get("id", ""))
    body = str(record.get("body", "") or "")
    score = float(record.get("score", 0) or 0)
    ups = float(record.get("ups", 0) or 0)
    downs = float(record.get("downs", 0) or 0)
    controversiality = int(record.get("controversiality", 0) or 0)
    return (
        run_id, str(uuid.uuid4()), comment_id,
        dt.strftime("%Y-%m-%d"), dt.year, dt.month,
        body, score, ups, downs, controversiality,
    )


def main():
    # Delete old DB for a clean start
    if DB_PATH.exists():
        os.remove(DB_PATH)
        print(f"Deleted old DB: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    create_tables(conn)

    # Create initial run
    run_ts = datetime.utcnow().isoformat()
    cursor = conn.execute(
        "INSERT INTO reddit_runs (run_ts, source_path, records_scanned) VALUES (?, ?, ?)",
        (run_ts, "HISTORIC_LOAD", 0),
    )
    run_id = cursor.lastrowid
    print(f"Created run_id={run_id}")

    total_rows = 0
    total_comment_rows = 0
    batch = []
    comment_batch = []
    BATCH_SIZE = 2000

    for dir_path, is_comment in DIRS:
        if not dir_path.exists():
            print(f"  Skipping {dir_path} (not found)")
            continue
        kind = "comments" if is_comment else "posts"
        print(f"\nLoading {kind} from {dir_path.name}/")

        for file_path in sorted(dir_path.glob("*.jsonl")):
            print(f"  {file_path.name} ...", end="", flush=True)
            file_rows = 0

            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    row = parse_fact_row(record, run_id, is_comment)
                    if row is None:
                        continue
                    batch.append(row)
                    file_rows += 1
                    total_rows += 1

                    # Also insert into reddit_comment_facts for comment analysis
                    if is_comment:
                        crow = parse_comment_fact_row(record, run_id)
                        if crow is not None:
                            comment_batch.append(crow)
                            total_comment_rows += 1

                    if len(batch) >= BATCH_SIZE:
                        conn.executemany(INSERT_SQL, batch)
                        batch.clear()
                    if len(comment_batch) >= BATCH_SIZE:
                        conn.executemany(COMMENT_INSERT_SQL, comment_batch)
                        comment_batch.clear()
                        conn.commit()

            print(f" {file_rows:,} rows")

    if batch:
        conn.executemany(INSERT_SQL, batch)
    if comment_batch:
        conn.executemany(COMMENT_INSERT_SQL, comment_batch)
    conn.commit()
    print(f"\nComment facts: {total_comment_rows:,} rows loaded into reddit_comment_facts")

    conn.execute(
        "UPDATE reddit_runs SET records_scanned = ? WHERE run_id = ?",
        (total_rows, run_id),
    )
    conn.commit()
    conn.close()

    print(f"\n{'='*50}")
    print(f"Done! Loaded {total_rows:,} total rows into run_id={run_id}")
    print(f"DB: {DB_PATH}")
    print(f"\nNext steps:")
    print(f"  1. python comments_post_producer.py   (start fetching live data)")
    print(f"  2. python spark_reddit_metrics.py      (start streaming processor)")
    print(f"  3. uvicorn main:app --reload           (start API)")
    print(f"  4. cd frontend && npm run dev          (start dashboard)")


if __name__ == "__main__":
    main()
