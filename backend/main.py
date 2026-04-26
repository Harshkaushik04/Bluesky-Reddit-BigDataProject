from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "backend" / "reddit_dashboard.db"
BLUESKY_DIR = BASE_DIR / "Bluesky_data" / "initial_firehose"
WORD_PATTERN = re.compile(r"[a-zA-Z]{3,}")

STOPWORDS = {
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
}

app = FastAPI(title="Reddit Dashboard API", version="0.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_latest_snapshot(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT run_id, run_ts, records_scanned, source_path
        FROM reddit_runs
        ORDER BY run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return {"run_id": row[0], "run_ts": row[1], "records_scanned": row[2], "source_path": row[3]}


def _parse_months(months: str | None) -> list[int]:
    if not months:
        return []
    parsed: list[int] = []
    for token in months.split(","):
        value = int(token.strip())
        if value < 1 or value > 12:
            raise ValueError("Month values must be between 1 and 12.")
        parsed.append(value)
    return sorted(set(parsed))


def _build_where_clause(run_id: int, year: str, months: list[int]) -> tuple[str, list[Any]]:
    clauses = ["run_id = ?"]
    params: list[Any] = [run_id]
    if year != "overall":
        clauses.append("year = ?")
        params.append(int(year))
    if months:
        clauses.append(f"month IN ({','.join('?' for _ in months)})")
        params.extend(months)
    return " AND ".join(clauses), params


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _classify_bluesky_post_type(record: dict[str, Any]) -> str:
    embed = record.get("embed")
    text = str(record.get("text", "")).strip()

    if not isinstance(embed, dict):
        return "text" if text else "other"

    embed_type = str(embed.get("$type", "")).lower()
    if "video" in embed_type:
        return "video"
    if "images" in embed_type or "image" in embed_type:
        return "photo"

    external = embed.get("external")
    if isinstance(external, dict):
        uri = str(external.get("uri", "")).lower()
        thumb = external.get("thumb")
        thumb_mime = ""
        if isinstance(thumb, dict):
            thumb_mime = str(thumb.get("mimeType", "")).lower()

        if any(token in uri for token in [".mp4", ".mov", ".webm", "video"]):
            return "video"
        if any(token in uri for token in [".jpg", ".jpeg", ".png", ".gif", ".webp", "image"]):
            return "photo"
        if thumb_mime.startswith("video/"):
            return "video"
        if thumb_mime.startswith("image/"):
            return "photo"

    return "other"


def _parse_bluesky_events() -> list[dict[str, Any]]:
    if not BLUESKY_DIR.exists():
        return []

    events: list[dict[str, Any]] = []
    for file_path in sorted(BLUESKY_DIR.glob("*.jsonl")):
        with file_path.open("r", encoding="utf-8") as input_file:
            for line in input_file:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue

                commit = payload.get("commit") or {}
                collection = commit.get("collection")
                if collection not in {
                    "app.bsky.feed.post",
                    "app.bsky.feed.like",
                    "app.bsky.graph.follow",
                }:
                    continue

                record = commit.get("record") or {}
                dt = _parse_iso_datetime(record.get("createdAt"))
                if dt is None:
                    continue
                if dt.year < 2025 or dt.year > 2026:
                    continue

                events.append(
                    {
                        "collection": collection,
                        "created_date": dt.strftime("%Y-%m-%d"),
                        "year": dt.year,
                        "month": dt.month,
                        "text": record.get("text", ""),
                        "post_type": _classify_bluesky_post_type(record)
                        if collection == "app.bsky.feed.post"
                        else None,
                    }
                )
    return events


def _filter_bluesky_events(events: list[dict[str, Any]], year: str, months: list[int]) -> list[dict[str, Any]]:
    filtered = events
    if year != "overall":
        filtered = [event for event in filtered if event["year"] == int(year)]
    if months:
        month_set = set(months)
        filtered = [event for event in filtered if event["month"] in month_set]
    return filtered


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/reddit/overview")
def reddit_overview(
    year: str = Query(default="overall"),
    months: str | None = Query(default=None, description="Comma-separated months: 1,2,3"),
) -> dict[str, Any]:
    if not DB_PATH.exists():
        raise HTTPException(status_code=404, detail="Database not found. Run backend/spark_reddit_metrics.py first.")

    with sqlite3.connect(DB_PATH) as conn:
        snapshot = _get_latest_snapshot(conn)
        if snapshot is None:
            raise HTTPException(status_code=404, detail="No snapshots found. Run backend/spark_reddit_metrics.py first.")

        run_id = int(snapshot["run_id"])
        available_years = [
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT year FROM reddit_post_facts WHERE run_id = ? ORDER BY year ASC",
                (run_id,),
            ).fetchall()
        ]
        if year != "overall":
            if not year.isdigit():
                raise HTTPException(status_code=400, detail="Invalid year. Use overall, 2025, or 2026.")
            if int(year) not in available_years:
                raise HTTPException(status_code=400, detail=f"Year {year} not available in this snapshot.")

        try:
            selected_months = _parse_months(months)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error

        where_clause, where_params = _build_where_clause(run_id, year, selected_months)
        fact_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(reddit_post_facts)").fetchall()
        }
        has_post_type = "post_type" in fact_columns

        agg_row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total_posts,
                COALESCE(AVG(score), 0) AS avg_score,
                COALESCE(AVG(num_comments), 0) AS avg_comments,
                COALESCE(SUM(engagement), 0) AS total_engagement,
                COUNT(DISTINCT created_date) AS active_days
            FROM reddit_post_facts
            WHERE {where_clause}
            """,
            where_params,
        ).fetchone()

        total_posts = int(agg_row[0]) if agg_row else 0
        avg_score = float(agg_row[1]) if agg_row else 0.0
        avg_comments = float(agg_row[2]) if agg_row else 0.0
        total_engagement = float(agg_row[3]) if agg_row else 0.0
        active_days = int(agg_row[4]) if agg_row else 0
        avg_engagement_per_day = (total_engagement / active_days) if active_days else 0.0

        timeline_rows = conn.execute(
            f"""
            SELECT created_date AS bucket, COUNT(*) AS count
            FROM reddit_post_facts
            WHERE {where_clause}
            GROUP BY created_date
            ORDER BY created_date ASC
            """,
            where_params,
        ).fetchall()
        timeline_series_rows = conn.execute(
            f"""
            SELECT
                created_date AS bucket,
                COUNT(*) AS posts,
                COALESCE(SUM(ups), 0) AS upvotes,
                COALESCE(SUM(num_comments), 0) AS comments,
                COALESCE(SUM(engagement), 0) AS total
            FROM reddit_post_facts
            WHERE {where_clause}
            GROUP BY created_date
            ORDER BY created_date ASC
            """,
            where_params,
        ).fetchall()

        title_rows = conn.execute(
            f"SELECT title FROM reddit_post_facts WHERE {where_clause}",
            where_params,
        ).fetchall()
        keyword_counter: Counter[str] = Counter()
        for (title,) in title_rows:
            if not title:
                continue
            for token in WORD_PATTERN.findall(str(title).lower()):
                if token not in STOPWORDS:
                    keyword_counter[token] += 1
        top_keywords = [{"label": word, "value": count} for word, count in keyword_counter.most_common(6)]
        if has_post_type:
            post_type_rows = conn.execute(
                f"""
                SELECT COALESCE(post_type, 'other') AS label, COUNT(*) AS value
                FROM reddit_post_facts
                WHERE {where_clause}
                GROUP BY COALESCE(post_type, 'other')
                ORDER BY value DESC
                """,
                where_params,
            ).fetchall()
        else:
            post_type_rows = [("other", total_posts)]
        total_posts_for_type = max(sum(int(row[1]) for row in post_type_rows), 1)
        post_type_split = [
            {
                "label": str(row[0]).lower(),
                "value": int(row[1]),
                "percent": round((int(row[1]) * 100.0) / total_posts_for_type, 2),
            }
            for row in post_type_rows
        ]

        return {
            "meta": {
                "run_id": run_id,
                "run_ts": snapshot["run_ts"],
                "records_scanned": snapshot["records_scanned"],
                "source_dir": snapshot["source_path"],
                "streaming_mode": "without_streaming",
                "data_source": "sql_database",
                "selected_year": year,
                "selected_months": selected_months,
                "available_years": available_years,
            },
            "kpis": {
                "total_posts": total_posts,
                "total_engagement": round(total_engagement, 2),
                "avg_engagement_per_day": round(avg_engagement_per_day, 2),
                "avg_sentiment": 0.0,
                "avg_score": round(avg_score, 2),
                "avg_comments": round(avg_comments, 2),
            },
            "content_split": {"posts": total_posts, "comments": 0},
            "post_type_split": post_type_split,
            "top_keywords": top_keywords,
            "timeline": [{"bucket": row[0], "count": row[1]} for row in timeline_rows],
            "timeline_series": [
                {
                    "bucket": row[0],
                    "posts": int(row[1]),
                    "upvotes": int(row[2]),
                    "comments": int(row[3]),
                    "total": int(row[4]),
                }
                for row in timeline_series_rows
            ],
        }


@app.get("/api/bluesky/overview")
def bluesky_overview(
    year: str = Query(default="overall"),
    months: str | None = Query(default=None, description="Comma-separated months: 1,2,3"),
) -> dict[str, Any]:
    all_events = _parse_bluesky_events()
    if not all_events:
        raise HTTPException(
            status_code=404,
            detail="No Bluesky data found. Put .jsonl files in Bluesky_data/initial_firehose.",
        )

    available_years = sorted({event["year"] for event in all_events})
    if year != "overall":
        if not year.isdigit():
            raise HTTPException(status_code=400, detail="Invalid year. Use overall, 2025, or 2026.")

    try:
        selected_months = _parse_months(months)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    filtered_events = _filter_bluesky_events(all_events, year, selected_months)

    posts = [event for event in filtered_events if event["collection"] == "app.bsky.feed.post"]
    likes = [event for event in filtered_events if event["collection"] == "app.bsky.feed.like"]
    follows = [event for event in filtered_events if event["collection"] == "app.bsky.graph.follow"]

    total_posts = len(posts)
    total_likes = len(likes)
    total_follows = len(follows)
    total_engagement = total_posts + total_likes + total_follows

    active_days = len({event["created_date"] for event in filtered_events})
    avg_engagement_per_day = (total_engagement / active_days) if active_days else 0.0

    timeline_counter: Counter[str] = Counter()
    timeline_by_type: dict[str, dict[str, int]] = {}
    for event in filtered_events:
        date_key = event["created_date"]
        timeline_counter[date_key] += 1
        if date_key not in timeline_by_type:
            timeline_by_type[date_key] = {"posts": 0, "likes": 0, "follows": 0}
        if event["collection"] == "app.bsky.feed.post":
            timeline_by_type[date_key]["posts"] += 1
        elif event["collection"] == "app.bsky.feed.like":
            timeline_by_type[date_key]["likes"] += 1
        elif event["collection"] == "app.bsky.graph.follow":
            timeline_by_type[date_key]["follows"] += 1

    timeline = [{"bucket": day, "count": timeline_counter[day]} for day in sorted(timeline_counter.keys())]
    timeline_series = [
        {
            "bucket": day,
            "posts": timeline_by_type[day]["posts"],
            "likes": timeline_by_type[day]["likes"],
            "follows": timeline_by_type[day]["follows"],
            "total": timeline_counter[day],
        }
        for day in sorted(timeline_by_type.keys())
    ]

    keyword_counter: Counter[str] = Counter()
    for post in posts:
        text = str(post.get("text", "")).lower()
        for token in WORD_PATTERN.findall(text):
            if token not in STOPWORDS:
                keyword_counter[token] += 1
    top_keywords = [{"label": word, "value": count} for word, count in keyword_counter.most_common(6)]
    post_type_counter: Counter[str] = Counter()
    for post in posts:
        post_type_counter[str(post.get("post_type") or "other")] += 1

    total_posts_for_type = max(sum(post_type_counter.values()), 1)
    post_type_split = []
    for label in ["video", "photo", "text", "other"]:
        value = int(post_type_counter.get(label, 0))
        percent = (value * 100.0) / total_posts_for_type
        post_type_split.append({"label": label, "value": value, "percent": round(percent, 2)})

    return {
        "meta": {
            "records_scanned": len(all_events),
            "source_dir": str(BLUESKY_DIR),
            "streaming_mode": "without_streaming",
            "data_source": "jsonl_files",
            "selected_year": year,
            "selected_months": selected_months,
            "available_years": available_years,
        },
        "kpis": {
            "total_posts": total_posts,
            "total_engagement": total_engagement,
            "avg_engagement_per_day": round(avg_engagement_per_day, 2),
            "avg_sentiment": 0.0,
            "avg_score": 0.0,
            "avg_comments": 0.0,
        },
        "content_split": {"posts": total_posts, "comments": total_likes + total_follows},
        "post_type_split": post_type_split,
        "top_keywords": top_keywords,
        "timeline": timeline,
        "timeline_series": timeline_series,
    }
