from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://backend_user:supersecretpassword@localhost:5432/bluesky_db",
)
MODEL_PATH = os.getenv(
    "LLM_MODEL_PATH",
    "D:/Bluesky-Reddit-BigDataProject/models/gemma-4-E4B-it-Q4_K_M.gguf",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
app = FastAPI(title="Bluesky Reddit Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_llm_instance = None


def to_iso(dt: datetime) -> str:
    return dt.isoformat(sep=" ", timespec="seconds")


def parse_ts(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


class TimeRangeRequest(BaseModel):
    range_from: str = Field(alias="range-from")
    range_to: str = Field(alias="range-to")


class SentimentsRequest(TimeRangeRequest):
    word: str


class PopularWordsRequest(TimeRangeRequest):
    num_words: int


class WordPopularityRequest(TimeRangeRequest):
    word: str


class ActionRecommendRequest(BaseModel):
    sentence: str


class ControversialTopicsRequest(TimeRangeRequest):
    top_n_words: int


class TrendSaturationRequest(TimeRangeRequest):
    top_n_words: int


class TopCrossTopicsRequest(BaseModel):
    top_n_topics: int


def get_llm():
    global _llm_instance
    if _llm_instance is not None:
        return _llm_instance
    try:
        from llama_cpp import Llama  # type: ignore

        _llm_instance = Llama(model_path=MODEL_PATH, n_ctx=2048, verbose=False)
        return _llm_instance
    except Exception:
        return None


@app.post("/getDataCollectedStats")
def get_data_collected_stats(payload: TimeRangeRequest) -> dict[str, Any]:
    stmt = text(
        """
        SELECT time_bucket, source_type, SUM(record_count) AS record_count
        FROM ingestion_metrics_timeline
        WHERE time_bucket BETWEEN :range_from AND :range_to
        GROUP BY time_bucket, source_type
        ORDER BY time_bucket ASC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            stmt,
            {"range_from": parse_ts(payload.range_from), "range_to": parse_ts(payload.range_to)},
        ).fetchall()

    firehose: list[dict[str, int]] = []
    get_posts: list[dict[str, int]] = []
    for row in rows:
        entry = {to_iso(row.time_bucket): int(row.record_count)}
        if str(row.source_type).lower() == "firehose":
            firehose.append(entry)
        else:
            get_posts.append(entry)
    return {
        "firehose_collected": {"collected": firehose},
        "getPosts_collected": {"collected": get_posts},
    }


@app.post("/getSentiments")
def get_sentiments(payload: SentimentsRequest) -> dict[str, Any]:
    stmt = text(
        """
        SELECT time_range, AVG(avg_vader_sentiment_score) AS sentiment_score
        FROM word_time_series
        WHERE lower(word) = lower(:word)
          AND time_range BETWEEN :range_from AND :range_to
        GROUP BY time_range
        ORDER BY time_range ASC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            stmt,
            {
                "word": payload.word,
                "range_from": parse_ts(payload.range_from),
                "range_to": parse_ts(payload.range_to),
            },
        ).fetchall()
    sentiments = [{to_iso(r.time_range): float(r.sentiment_score)} for r in rows]
    return {"sentiments": sentiments}


@app.post("/popularWordsByTime")
def popular_words_by_time(payload: PopularWordsRequest) -> dict[str, Any]:
    top_words_stmt = text(
        """
        SELECT word, SUM(word_count) AS total_count
        FROM word_time_series
        WHERE time_range BETWEEN :range_from AND :range_to
        GROUP BY word
        ORDER BY total_count DESC
        LIMIT :num_words
        """
    )
    series_stmt = text(
        """
        SELECT word, time_range, SUM(word_count) AS popularity
        FROM word_time_series
        WHERE word = ANY(:words)
          AND time_range BETWEEN :range_from AND :range_to
        GROUP BY word, time_range
        ORDER BY time_range ASC
        """
    )
    with engine.connect() as conn:
        top_rows = conn.execute(
            top_words_stmt,
            {
                "range_from": parse_ts(payload.range_from),
                "range_to": parse_ts(payload.range_to),
                "num_words": payload.num_words,
            },
        ).fetchall()
        words = [r.word for r in top_rows]
        if not words:
            return {"words": []}
        rows = conn.execute(
            series_stmt,
            {
                "words": words,
                "range_from": parse_ts(payload.range_from),
                "range_to": parse_ts(payload.range_to),
            },
        ).fetchall()

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row.word].append(
            {"time_range": to_iso(row.time_range), "popularity": int(row.popularity)}
        )
    return {"words": [{"word": w, "popularity": grouped[w]} for w in words]}


@app.post("/wordPopularityTimeline")
def word_popularity_timeline(payload: WordPopularityRequest) -> dict[str, Any]:
    stmt = text(
        """
        SELECT time_range, SUM(word_count) AS word_count
        FROM word_time_series
        WHERE lower(word) = lower(:word)
          AND time_range BETWEEN :range_from AND :range_to
        GROUP BY time_range
        ORDER BY time_range ASC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            stmt,
            {
                "word": payload.word,
                "range_from": parse_ts(payload.range_from),
                "range_to": parse_ts(payload.range_to),
            },
        ).fetchall()
    return {"popularity": [{to_iso(r.time_range): int(r.word_count)} for r in rows]}


@app.post("/actionRecommend")
def action_recommend(payload: ActionRecommendRequest) -> dict[str, str]:
    words = [w.strip(".,!?;:()[]{}\"'").lower() for w in payload.sentence.split() if w.strip()]
    if not words:
        raise HTTPException(status_code=400, detail="Sentence must include at least one word.")

    stmt = text(
        """
        SELECT DISTINCT ON (lower(word)) lower(word) AS word, avg_vader_sentiment_score
        FROM word_time_series
        WHERE lower(word) = ANY(:words)
        ORDER BY lower(word), time_range DESC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(stmt, {"words": words}).fetchall()
    sentiment_map = {r.word: float(r.avg_vader_sentiment_score) for r in rows}

    sentiment_lines = [f"{w}: {sentiment_map.get(w, 0.0):.4f}" for w in words]
    prompt = (
        "You are a social-media posting assistant.\n"
        "Given this sentence and word-level sentiment signals, answer in one short line "
        "whether the user should post this sentence on Bluesky.\n\n"
        f"Sentence: {payload.sentence}\n"
        "Word sentiments:\n"
        + "\n".join(sentiment_lines)
        + "\n\nFormat: Recommendation: <Post/Do not post> - <short reason>"
    )

    llm = get_llm()
    if llm is None:
        avg_score = sum(sentiment_map.get(w, 0.0) for w in words) / max(len(words), 1)
        fallback = (
            "Recommendation: Post - overall sentiment is positive."
            if avg_score >= 0
            else "Recommendation: Do not post - overall sentiment trends negative."
        )
        return {"response": fallback}

    output = llm(prompt, max_tokens=80, temperature=0.2)
    text_out = output["choices"][0]["text"].strip()
    return {"response": text_out}


@app.post("/getControversialTopics")
def get_controversial_topics(payload: ControversialTopicsRequest) -> dict[str, Any]:
    stmt = text(
        """
        WITH ranked AS (
            SELECT
                time_bucket,
                topic_name,
                average_like_to_comment_ratio,
                ROW_NUMBER() OVER (
                    PARTITION BY time_bucket
                    ORDER BY average_like_to_comment_ratio DESC
                ) AS rn
            FROM controversial_topics_timeline
            WHERE time_bucket BETWEEN :range_from AND :range_to
        )
        SELECT time_bucket, topic_name, average_like_to_comment_ratio
        FROM ranked
        WHERE rn <= :top_n_words
        ORDER BY time_bucket ASC, average_like_to_comment_ratio DESC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            stmt,
            {
                "range_from": parse_ts(payload.range_from),
                "range_to": parse_ts(payload.range_to),
                "top_n_words": payload.top_n_words,
            },
        ).fetchall()

    by_range: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_range[to_iso(row.time_bucket)].append(
            {
                "topic_name": row.topic_name,
                "average_like_to_comment_ratio": float(row.average_like_to_comment_ratio),
            }
        )
    return {
        "ranges": [{"range": k, "topics": v} for k, v in sorted(by_range.items(), key=lambda x: x[0])]
    }


@app.post("/getWordControversyTimeline")
def get_word_controversy_timeline(payload: WordPopularityRequest) -> dict[str, Any]:
    stmt = text(
        """
        SELECT time_bucket, average_like_to_comment_ratio
        FROM controversial_topics_timeline
        WHERE lower(topic_name) = lower(:word)
          AND time_bucket BETWEEN :range_from AND :range_to
        ORDER BY time_bucket ASC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            stmt,
            {
                "word": payload.word,
                "range_from": parse_ts(payload.range_from),
                "range_to": parse_ts(payload.range_to),
            },
        ).fetchall()
    return {"controversy": [{to_iso(r.time_bucket): float(r.average_like_to_comment_ratio)} for r in rows]}


@app.post("/getTrendSaturation")
def get_trend_saturation(payload: TrendSaturationRequest) -> dict[str, Any]:
    slope_stmt = text(
        """
        WITH per_word AS (
            SELECT
                word,
                EXTRACT(EPOCH FROM time_range) AS ts_epoch,
                word_count
            FROM word_time_series
            WHERE time_range BETWEEN :range_from AND :range_to
        ),
        regression AS (
            SELECT
                word,
                regr_slope(word_count, ts_epoch) AS slope
            FROM per_word
            GROUP BY word
        )
        SELECT word
        FROM regression
        ORDER BY slope ASC
        LIMIT :top_n_words
        """
    )
    series_stmt = text(
        """
        SELECT word, time_range, SUM(word_count) AS word_count
        FROM word_time_series
        WHERE word = ANY(:words)
          AND time_range BETWEEN :range_from AND :range_to
        GROUP BY word, time_range
        ORDER BY time_range ASC
        """
    )
    with engine.connect() as conn:
        words_rows = conn.execute(
            slope_stmt,
            {
                "range_from": parse_ts(payload.range_from),
                "range_to": parse_ts(payload.range_to),
                "top_n_words": payload.top_n_words,
            },
        ).fetchall()
        words = [r.word for r in words_rows]
        if not words:
            return {"data": {}}
        rows = conn.execute(
            series_stmt,
            {
                "words": words,
                "range_from": parse_ts(payload.range_from),
                "range_to": parse_ts(payload.range_to),
            },
        ).fetchall()

    by_word: dict[str, list[dict[str, int]]] = defaultdict(list)
    for row in rows:
        by_word[row.word].append({to_iso(row.time_range): int(row.word_count)})
    return {"data": dict(by_word)}


@app.post("/getTopCrossTopics")
def get_top_cross_topics(payload: TopCrossTopicsRequest) -> dict[str, Any]:
    stmt = text(
        """
        SELECT topic_name, SUM(reddit_link_count) AS links
        FROM reddit_crossover_stats
        GROUP BY topic_name
        ORDER BY links DESC
        LIMIT :top_n_topics
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(stmt, {"top_n_topics": payload.top_n_topics}).fetchall()
    return {"topics": {r.topic_name: int(r.links) for r in rows}}

