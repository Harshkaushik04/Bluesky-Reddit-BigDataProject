from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "backend" / "reddit_dashboard.db"
BLUESKY_DIR = BASE_DIR / "Bluesky_data" / "initial_firehose"
WORD_PATTERN = re.compile(r"[a-zA-Z]{3,}")
POSITIVE_WORDS = {
    "good",
    "great",
    "best",
    "love",
    "awesome",
    "excellent",
    "positive",
    "support",
    "happy",
    "success",
    "win",
    "improve",
    "growth",
    "safe",
}
NEGATIVE_WORDS = {
    "bad",
    "worst",
    "hate",
    "awful",
    "terrible",
    "negative",
    "angry",
    "fail",
    "loss",
    "risk",
    "issue",
    "problem",
    "crash",
    "decline",
}

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


def _tokenize_text(value: str | None) -> list[str]:
    if not value:
        return []
    return [token for token in WORD_PATTERN.findall(str(value).lower()) if token not in STOPWORDS]


def _linear_slope(points: list[tuple[int, float]]) -> float:
    n = len(points)
    if n < 2:
        return 0.0
    sum_x = sum(float(x) for x, _ in points)
    sum_y = sum(y for _, y in points)
    sum_xy = sum(float(x) * y for x, y in points)
    sum_x2 = sum(float(x) * float(x) for x, _ in points)
    denominator = (n * sum_x2) - (sum_x * sum_x)
    if denominator == 0:
        return 0.0
    return ((n * sum_xy) - (sum_x * sum_y)) / denominator


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
        comment_total_row = conn.execute(
            f"""
            SELECT COALESCE(SUM(num_comments), 0)
            FROM reddit_post_facts
            WHERE {where_clause}
            """,
            where_params,
        ).fetchone()
        total_comments_split = int(comment_total_row[0] or 0) if comment_total_row else 0

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
            "content_split": {"posts": total_posts, "comments": total_comments_split},
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


@app.get("/api/reddit/comments/overview")
def reddit_comments_overview(
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
        required_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('reddit_comment_facts','reddit_comment_metrics')"
            ).fetchall()
        }
        if "reddit_comment_facts" not in required_tables:
            raise HTTPException(status_code=404, detail="Comment tables missing. Re-run backend/spark_reddit_metrics.py.")

        available_years = [
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT year FROM reddit_comment_facts WHERE run_id = ? ORDER BY year ASC",
                (run_id,),
            ).fetchall()
        ]
        if year != "overall":
            if not year.isdigit():
                raise HTTPException(status_code=400, detail="Invalid year. Use overall, 2025, or 2026.")
            if int(year) not in available_years:
                return {
                    "meta": {
                        "run_id": run_id,
                        "run_ts": snapshot["run_ts"],
                        "selected_year": year,
                        "selected_months": [],
                        "available_years": available_years,
                    },
                    "kpis": {
                        "total_comments": 0,
                        "total_controversial_comments": 0,
                        "controversial_percent": 0.0,
                        "avg_comment_upvotes": 0.0,
                        "avg_comment_downvotes": 0.0,
                        "avg_comment_score": 0.0,
                    },
                    "timeline_series": [],
                    "controversial_split": [
                        {"label": "controversial", "value": 0, "percent": 0.0},
                        {"label": "non_controversial", "value": 0, "percent": 0.0},
                    ],
                    "score_split": [],
                }

        try:
            selected_months = _parse_months(months)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error

        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if year != "overall":
            clauses.append("year = ?")
            params.append(int(year))
        if selected_months:
            clauses.append(f"month IN ({','.join('?' for _ in selected_months)})")
            params.extend(selected_months)
        where_clause = " AND ".join(clauses)

        agg_row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total_comments,
                SUM(CASE WHEN controversiality > 0 THEN 1 ELSE 0 END) AS controversial_comments,
                COALESCE(AVG(ups), 0) AS avg_upvotes,
                COALESCE(AVG(downs), 0) AS avg_downvotes,
                COALESCE(AVG(score), 0) AS avg_score
            FROM reddit_comment_facts
            WHERE {where_clause}
            """,
            params,
        ).fetchone()

        total_comments = int(agg_row[0] or 0)
        controversial_comments = int(agg_row[1] or 0)
        avg_upvotes = float(agg_row[2] or 0.0)
        avg_downvotes = float(agg_row[3] or 0.0)
        avg_score = float(agg_row[4] or 0.0)
        controversial_pct = (controversial_comments * 100.0 / total_comments) if total_comments else 0.0

        timeline_rows = conn.execute(
            f"""
            SELECT
                created_date AS bucket,
                COUNT(*) AS total_comments,
                SUM(CASE WHEN controversiality > 0 THEN 1 ELSE 0 END) AS controversial_comments
            FROM reddit_comment_facts
            WHERE {where_clause}
            GROUP BY created_date
            ORDER BY created_date ASC
            """,
            params,
        ).fetchall()

        score_split_rows = conn.execute(
            f"""
            SELECT
                CASE
                    WHEN score > 0 THEN 'positive'
                    WHEN score < 0 THEN 'negative'
                    ELSE 'neutral'
                END AS label,
                COUNT(*) AS value
            FROM reddit_comment_facts
            WHERE {where_clause}
            GROUP BY label
            ORDER BY value DESC
            """,
            params,
        ).fetchall()
        score_total = max(sum(int(row[1]) for row in score_split_rows), 1)
        score_split = [
            {"label": str(row[0]), "value": int(row[1]), "percent": round(int(row[1]) * 100.0 / score_total, 2)}
            for row in score_split_rows
        ]

        controversial_split = [
            {
                "label": "controversial",
                "value": controversial_comments,
                "percent": round(controversial_pct, 2),
            },
            {
                "label": "non_controversial",
                "value": max(total_comments - controversial_comments, 0),
                "percent": round(max(100.0 - controversial_pct, 0.0), 2),
            },
        ]

        return {
            "meta": {
                "run_id": run_id,
                "run_ts": snapshot["run_ts"],
                "selected_year": year,
                "selected_months": selected_months,
                "available_years": available_years,
            },
            "kpis": {
                "total_comments": total_comments,
                "total_controversial_comments": controversial_comments,
                "controversial_percent": round(controversial_pct, 2),
                "avg_comment_upvotes": round(avg_upvotes, 2),
                "avg_comment_downvotes": round(avg_downvotes, 2),
                "avg_comment_score": round(avg_score, 2),
            },
            "timeline_series": [
                {
                    "bucket": row[0],
                    "total_comments": int(row[1] or 0),
                    "controversial_comments": int(row[2] or 0),
                    "normal_comments": int((row[1] or 0) - (row[2] or 0)),
                    "total": int(row[1] or 0),
                }
                for row in timeline_rows
            ],
            "controversial_split": controversial_split,
            "score_split": score_split,
        }


@app.get("/api/reddit/feature-insights")
def reddit_feature_insights(
    year: str = Query(default="overall"),
    months: str | None = Query(default=None, description="Comma-separated months: 1,2,3"),
    word: str | None = Query(default=None, description="Optional word for popularity timeline"),
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
        rows = conn.execute(
            f"""
            SELECT created_date, title, score, ups, num_comments
            FROM reddit_post_facts
            WHERE {where_clause}
            ORDER BY created_date ASC
            """,
            where_params,
        ).fetchall()

        if not rows:
            return {
                "meta": {
                    "run_id": run_id,
                    "run_ts": snapshot["run_ts"],
                    "selected_year": year,
                    "selected_months": selected_months,
                    "available_years": available_years,
                },
                "sentiment_kpis": {"avg_sentiment": 0.0, "positive_posts": 0, "negative_posts": 0, "neutral_posts": 0},
                "sentiment_timeline": [],
                "word_popularity": {"top_words": [], "timeline": []},
                "controversial_topics": [],
                "trend_saturation": {"summary": [], "timeline": []},
            }

        day_sentiment: dict[str, dict[str, float]] = defaultdict(
            lambda: {"positive": 0.0, "negative": 0.0, "neutral": 0.0, "sum_sentiment": 0.0, "count": 0.0}
        )
        word_day_count: dict[str, Counter[str]] = defaultdict(Counter)
        word_total_count: Counter[str] = Counter()
        word_controversy_total: dict[str, float] = defaultdict(float)
        word_controversy_count: Counter[str] = Counter()

        total_sentiment_sum = 0.0
        positive_posts = 0
        negative_posts = 0
        neutral_posts = 0

        for created_date, title, score, ups, num_comments in rows:
            tokens = _tokenize_text(title)
            if not tokens:
                sentiment_value = 0.0
            else:
                pos_hits = sum(1 for token in tokens if token in POSITIVE_WORDS)
                neg_hits = sum(1 for token in tokens if token in NEGATIVE_WORDS)
                sentiment_value = (pos_hits - neg_hits) / max(len(tokens), 1)

            total_sentiment_sum += sentiment_value
            if sentiment_value > 0:
                positive_posts += 1
                day_sentiment[str(created_date)]["positive"] += 1
            elif sentiment_value < 0:
                negative_posts += 1
                day_sentiment[str(created_date)]["negative"] += 1
            else:
                neutral_posts += 1
                day_sentiment[str(created_date)]["neutral"] += 1

            day_sentiment[str(created_date)]["sum_sentiment"] += sentiment_value
            day_sentiment[str(created_date)]["count"] += 1

            controversy_score = float(num_comments or 0.0) / (abs(float(score or 0.0)) + 1.0)
            for token in set(tokens):
                word_total_count[token] += 1
                word_day_count[token][str(created_date)] += 1
                word_controversy_total[token] += controversy_score
                word_controversy_count[token] += 1

        sentiment_timeline = []
        for day in sorted(day_sentiment.keys()):
            row = day_sentiment[day]
            avg_value = row["sum_sentiment"] / max(row["count"], 1.0)
            sentiment_timeline.append(
                {
                    "bucket": day,
                    "positive_posts": int(row["positive"]),
                    "negative_posts": int(row["negative"]),
                    "neutral_posts": int(row["neutral"]),
                    "avg_sentiment": round(avg_value, 4),
                    "total": int(row["count"]),
                }
            )

        requested_word = (word or "").strip().lower()
        if requested_word:
            top_words = [requested_word]
        else:
            top_words = [token for token, _ in word_total_count.most_common(5)]
        word_popularity_timeline = []
        all_days = sorted({str(row[0]) for row in rows})
        for day in all_days:
            entry: dict[str, Any] = {"bucket": day, "total": 0}
            for token in top_words:
                value = int(word_day_count[token].get(day, 0))
                entry[token] = value
                entry["total"] += value
            word_popularity_timeline.append(entry)

        controversial_topics = []
        for token, freq in word_total_count.most_common(12):
            avg_controversy = word_controversy_total[token] / max(word_controversy_count[token], 1)
            controversial_topics.append(
                {
                    "topic": token,
                    "mentions": int(freq),
                    "controversy_score": round(avg_controversy, 3),
                }
            )
        controversial_topics.sort(key=lambda item: item["controversy_score"], reverse=True)
        controversial_topics = controversial_topics[:8]

        trend_summary = []
        trend_candidates = [token for token, _ in word_total_count.most_common(25)]
        for trend_word in trend_candidates:
            series = [int(word_day_count[trend_word].get(day, 0)) for day in all_days]
            if len(series) < 3:
                continue
            points = list(enumerate(float(value) for value in series))
            slope = _linear_slope(points)
            peak = max(series) if series else 0
            latest = series[-1] if series else 0
            peak_idx = series.index(peak) if peak else 0
            avg_recent = (series[-1] + series[-2]) / 2.0 if len(series) >= 2 else float(latest)
            split_idx = max(1, len(series) // 2)
            first_half_avg = sum(series[:split_idx]) / max(split_idx, 1)
            second_half_avg = sum(series[split_idx:]) / max(len(series) - split_idx, 1)
            saturation_ratio = (latest / peak) if peak else 0.0
            is_dying = (
                peak >= 3
                and peak_idx <= (len(series) - 3)
                and avg_recent <= (peak * 0.55)
                and slope < -0.02
                and second_half_avg < (first_half_avg * 0.85)
            )
            if not is_dying:
                continue
            trend_summary.append(
                {
                    "topic": trend_word,
                    "slope": round(slope, 3),
                    "latest": int(latest),
                    "peak": int(peak),
                    "saturation_ratio": round(saturation_ratio, 3),
                    "status": "dying",
                }
            )
        trend_summary.sort(key=lambda item: (item["slope"], item["saturation_ratio"]))
        trend_summary = trend_summary[:8]
        saturation_words = [row["topic"] for row in trend_summary[:3]]
        saturation_timeline = []
        for day in all_days:
            entry: dict[str, Any] = {"bucket": day, "total": 0}
            for trend_word in saturation_words:
                value = int(word_day_count[trend_word].get(day, 0))
                entry[trend_word] = value
                entry["total"] += value
            saturation_timeline.append(entry)

        series_by_word: dict[str, list[dict[str, Any]]] = {}
        for row in trend_summary:
            topic = str(row["topic"])
            series_by_word[topic] = [
                {"bucket": day, "value": int(word_day_count[topic].get(day, 0)), "total": int(word_day_count[topic].get(day, 0))}
                for day in all_days
            ]

        total_posts = len(rows)
        avg_sentiment = total_sentiment_sum / max(total_posts, 1)
        return {
            "meta": {
                "run_id": run_id,
                "run_ts": snapshot["run_ts"],
                "selected_year": year,
                "selected_months": selected_months,
                "available_years": available_years,
            },
            "sentiment_kpis": {
                "avg_sentiment": round(avg_sentiment, 4),
                "positive_posts": int(positive_posts),
                "negative_posts": int(negative_posts),
                "neutral_posts": int(neutral_posts),
            },
            "sentiment_timeline": sentiment_timeline,
            "word_popularity": {
                "top_words": top_words,
                "selected_word": requested_word or None,
                "timeline": word_popularity_timeline,
            },
            "controversial_topics": controversial_topics,
            "trend_saturation": {
                "summary": trend_summary,
                "saturation_words": saturation_words,
                "timeline": saturation_timeline,
                "series_by_word": series_by_word,
            },
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
