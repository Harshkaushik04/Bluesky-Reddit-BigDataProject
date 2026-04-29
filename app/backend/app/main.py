from __future__ import annotations

import os
import logging
import subprocess
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=True)
logger = logging.getLogger("backend.llm")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://backend_user:supersecretpassword@localhost:5432/bluesky_db",
)
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://127.0.0.1:1234/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "").strip()
LLM_API_KEY = os.getenv("LLM_API_KEY", "lm-studio")
LLM_TIMEOUT_SECONDS = float(os.getenv("LLM_TIMEOUT_SECONDS", "120"))
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "all_posts_and_comments")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME", "all-MiniLM-L6-v2")
VECTORDATABASE_ENABLED = os.getenv("VECTORDATABASE_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
    "y",
}
QDRANT_ON_DEMAND = os.getenv("QDRANT_ON_DEMAND", "false").lower() in {"1", "true", "yes", "y"}
QDRANT_DOCKER_CONTAINER_NAME = os.getenv("QDRANT_DOCKER_CONTAINER_NAME", "qdrant")
QDRANT_DOCKER_IMAGE = os.getenv("QDRANT_DOCKER_IMAGE", "qdrant/qdrant:latest")
QDRANT_DOCKER_STORAGE_VOLUME = os.getenv("QDRANT_DOCKER_STORAGE_VOLUME", "qdrant_storage_local")
QDRANT_HOST_STORAGE_PATH = os.getenv("QDRANT_HOST_STORAGE_PATH", "").strip()
QDRANT_HOST_CONFIG_PATH = os.getenv("QDRANT_HOST_CONFIG_PATH", "").strip()
QDRANT_STARTUP_TIMEOUT_SECONDS = int(os.getenv("QDRANT_STARTUP_TIMEOUT_SECONDS", "30"))
QDRANT_STARTUP_POLL_INTERVAL_SECONDS = float(os.getenv("QDRANT_STARTUP_POLL_INTERVAL_SECONDS", "1"))
ALLOW_SENTIMENT_FALLBACK = os.getenv("ALLOW_SENTIMENT_FALLBACK", "false").lower() in {
    "1",
    "true",
    "yes",
}

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
_llm_model_in_use: str | None = None
_llm_load_error: str | None = None
_qdrant_client = None
_embedding_model = None
_vectordb_load_error: str | None = None


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


class WhySentimentsRequest(SentimentsRequest):
    sample_points: int = 24
    # If provided, VectorDB/Qdrant is skipped and these snippets are used instead.
    retrieved_texts: list[str] | None = None


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
    global _llm_instance, _llm_load_error, _llm_model_in_use
    if _llm_instance is not None:
        return _llm_instance
    try:
        headers = {"Authorization": f"Bearer {LLM_API_KEY}"} if LLM_API_KEY else {}
        client = httpx.Client(base_url=LLM_BASE_URL, timeout=LLM_TIMEOUT_SECONDS, headers=headers)

        selected_model = LLM_MODEL
        if not selected_model:
            models_response = client.get("models")
            models_response.raise_for_status()
            models_payload = models_response.json()
            data = models_payload.get("data", [])
            if not data:
                raise RuntimeError("LM Studio server returned no loaded models.")
            selected_model = data[0].get("id", "")
            if not selected_model:
                raise RuntimeError("LM Studio returned an invalid model entry.")

        _llm_instance = client
        _llm_model_in_use = selected_model
        _llm_load_error = None
        return _llm_instance
    except Exception as exc:
        _llm_load_error = f"{type(exc).__name__}: {exc}"
        _llm_model_in_use = None
        logger.exception(
            "Failed to initialize LLM client. base_url=%s configured_model=%s",
            LLM_BASE_URL,
            LLM_MODEL or "<auto>",
        )
        return None


def llm_chat_completion(messages: list[dict[str, str]], max_tokens: int, temperature: float) -> str:
    global _llm_load_error
    llm = get_llm()
    if llm is None or _llm_model_in_use is None:
        raise RuntimeError(_llm_load_error or "LLM is unavailable.")

    payload = {
        "model": _llm_model_in_use,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    try:
        response = llm.post("chat/completions", json=payload)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        _llm_load_error = f"{type(exc).__name__}: {exc}"
        logger.exception(
            "LLM completion call failed. base_url=%s model=%s max_tokens=%s temperature=%s",
            LLM_BASE_URL,
            _llm_model_in_use,
            max_tokens,
            temperature,
        )
        raise
    choice = (data.get("choices") or [{}])[0]
    message = choice.get("message", {}) if isinstance(choice, dict) else {}
    content = message.get("content", "") if isinstance(message, dict) else ""
    if isinstance(content, str) and content.strip():
        _llm_load_error = None
        return content.strip()

    # Some reasoning models may return reasoning_content with empty content.
    reasoning_content = message.get("reasoning_content", "") if isinstance(message, dict) else ""
    if isinstance(reasoning_content, str) and reasoning_content.strip():
        logger.warning("LLM returned empty content; using reasoning_content fallback text.")
        _llm_load_error = None
        return reasoning_content.strip()

    finish_reason = choice.get("finish_reason") if isinstance(choice, dict) else None
    raise RuntimeError(f"LLM returned empty content (finish_reason={finish_reason}).")


def deterministic_action_recommendation(avg_score: float, neg_frac: float, min_score: float) -> str:
    if avg_score <= -0.05 or neg_frac >= 0.40 or min_score <= -0.25:
        return (
            "Recommendation: Do not post - rule 1 triggered "
            f"(avg_score={avg_score:.4f}, neg_frac={neg_frac:.2f}, min_score={min_score:.4f})."
        )
    return (
        "Recommendation: Post - rule 2 triggered "
        f"(avg_score={avg_score:.4f}, neg_frac={neg_frac:.2f}, min_score={min_score:.4f})."
    )


def get_qdrant_client():
    global _qdrant_client, _vectordb_load_error
    if _qdrant_client is not None:
        return _qdrant_client
    try:
        from qdrant_client import QdrantClient  # type: ignore

        _qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, check_compatibility=False)
        _vectordb_load_error = None
        return _qdrant_client
    except Exception as exc:
        _vectordb_load_error = f"{type(exc).__name__}: {exc}"
        return None


def get_embedding_model():
    global _embedding_model, _vectordb_load_error
    if _embedding_model is not None:
        return _embedding_model
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore

        _embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        _vectordb_load_error = None
        return _embedding_model
    except Exception as exc:
        _vectordb_load_error = f"{type(exc).__name__}: {exc}"
        return None


def sample_timeseries(points: list[dict[str, float]], max_points: int) -> list[dict[str, float]]:
    if max_points <= 0:
        return []
    if len(points) <= max_points:
        return points
    # Evenly sample across the full range, always including first/last.
    idxs = sorted({0, len(points) - 1} | {round(i * (len(points) - 1) / (max_points - 1)) for i in range(max_points)})
    return [points[i] for i in idxs]


def vectordb_top_texts_for_word(
    word: str,
    limit: int = 5,
    *,
    min_text_length: int = 60,
    allow_disabled: bool = False,
) -> list[str]:
    global _vectordb_load_error
    if (not VECTORDATABASE_ENABLED) and (not allow_disabled):
        _vectordb_load_error = "disabled_by_env"
        return []
    client = get_qdrant_client()
    if client is None:
        return []

    # Fast-fail before loading embedding model: if Qdrant is down, skip VectorDB enrichment.
    try:
        client.get_collections()
    except Exception as exc:
        _vectordb_load_error = f"{type(exc).__name__}: {exc}"
        logger.warning("Skipping VectorDB retrieval because Qdrant is unreachable: %s", _vectordb_load_error)
        return []

    model = get_embedding_model()
    if model is None:
        return []

    query = (
        f"Context for sentiment around the word '{word}': what events, topics, or discussions mention it "
        "and what sentiment or controversy is expressed?"
    )
    # Embedding generation can fail due to model download/initialization issues.
    # Keep VectorDB enrichment best-effort so /why-sentiments doesn't crash the server.
    try:
        embedding = model.encode(query, show_progress_bar=False, normalize_embeddings=True)
    except Exception as exc:
        _vectordb_load_error = f"{type(exc).__name__}: {exc}"
        logger.exception("Embedding generation failed for vectordb query.")
        return []

    try:
        results = client.query_points(collection_name=QDRANT_COLLECTION, query=embedding, limit=50).points
        _vectordb_load_error = None
    except Exception as exc:
        # VectorDB is optional for the app; if it's down, degrade gracefully.
        _vectordb_load_error = f"{type(exc).__name__}: {exc}"
        return []
    out: list[str] = []
    for element in results:
        payload = getattr(element, "payload", None) or {}
        text_val = ""
        if isinstance(payload, dict):
            # Qdrant payload key names can vary depending on how you ingested data.
            for k in ("text", "content", "body", "comment", "post", "title"):
                v = payload.get(k)
                if isinstance(v, str):
                    text_val = v
                    break
        if isinstance(text_val, str) and len(text_val.strip()) >= min_text_length:
            out.append(text_val.strip())
        if len(out) >= limit:
            break
    return out


class RetrievePostsRequest(BaseModel):
    word: str
    limit: int = 5
    # Lower this if your stored payload texts are short and you still want non-empty results.
    min_text_length: int = 1


def _qdrant_wait_for_collection_loaded() -> None:
    # Poll until Qdrant reports the collection exists.
    deadline = time.time() + QDRANT_STARTUP_TIMEOUT_SECONDS
    collections_url = f"http://{QDRANT_HOST}:{QDRANT_PORT}/collections"
    while time.time() < deadline:
        try:
            resp = httpx.get(collections_url, timeout=5)
            resp.raise_for_status()
            payload = resp.json()
            names = [c.get("name") for c in payload.get("result", {}).get("collections", []) if isinstance(c, dict)]
            if QDRANT_COLLECTION in names:
                return
        except Exception:
            pass
        time.sleep(QDRANT_STARTUP_POLL_INTERVAL_SECONDS)
    # If the exact collection isn't present, don't hard-fail the whole request.
    # The caller can decide how to handle empty retrieval results.
    logger.warning(
        "Timed out waiting for Qdrant collection '%s' to load. QDRANT_HOST=%s:%s",
        QDRANT_COLLECTION,
        QDRANT_HOST,
        QDRANT_PORT,
    )


def _docker_start_qdrant_if_needed() -> bool:
    """
    Starts the qdrant container if QDRANT_ON_DEMAND is enabled.
    Returns True if we started it (caller should stop it later).
    """
    if not QDRANT_ON_DEMAND:
        return False

    # Check running state.
    ps = subprocess.run(
        ["docker", "ps", "-q", "-f", f"name={QDRANT_DOCKER_CONTAINER_NAME}"],
        capture_output=True,
        text=True,
    )
    is_running = bool(ps.stdout.strip())
    if is_running:
        return False

    container_id = subprocess.run(
        ["docker", "ps", "-aq", "-f", f"name={QDRANT_DOCKER_CONTAINER_NAME}"],
        capture_output=True,
        text=True,
    ).stdout.strip()

    if container_id:
        # If we use bind mounts, the container must be recreated to pick up mount changes.
        if QDRANT_HOST_STORAGE_PATH:
            subprocess.run(["docker", "rm", "-f", QDRANT_DOCKER_CONTAINER_NAME], capture_output=True, text=True)
        else:
            start = subprocess.run(
                ["docker", "start", QDRANT_DOCKER_CONTAINER_NAME],
                capture_output=True,
                text=True,
            )
            if start.returncode != 0:
                raise RuntimeError(f"docker start failed: {start.stderr.strip() or start.stdout.strip()}")
            return True

    docker_run_cmd: list[str] = [
        "docker",
        "run",
        "-d",
        "--name",
        QDRANT_DOCKER_CONTAINER_NAME,
        "-p",
        "6333:6333",
        "-p",
        "6334:6334",
    ]

    if QDRANT_HOST_STORAGE_PATH:
        docker_run_cmd += ["-v", f"{QDRANT_HOST_STORAGE_PATH}:/qdrant/storage"]
    else:
        docker_run_cmd += ["-v", f"{QDRANT_DOCKER_STORAGE_VOLUME}:/qdrant/storage"]

    if QDRANT_HOST_CONFIG_PATH:
        docker_run_cmd += ["-v", f"{QDRANT_HOST_CONFIG_PATH}:/qdrant/config"]

    docker_run_cmd.append(QDRANT_DOCKER_IMAGE)

    run = subprocess.run(docker_run_cmd, capture_output=True, text=True)
    if run.returncode != 0:
        raise RuntimeError(f"docker run failed: {run.stderr.strip() or run.stdout.strip()}")
    return True


def _docker_stop_qdrant_if_started(started_by_us: bool) -> None:
    if not started_by_us:
        return
    subprocess.run(["docker", "stop", QDRANT_DOCKER_CONTAINER_NAME], capture_output=True, text=True)


@app.post("/retrievePosts")
def retrieve_posts(payload: RetrievePostsRequest) -> dict[str, Any]:
    started = False
    qdrant_collections: list[str] = []
    try:
        started = _docker_start_qdrant_if_needed()
        _qdrant_wait_for_collection_loaded()
        try:
            resp = httpx.get(f"http://{QDRANT_HOST}:{QDRANT_PORT}/collections", timeout=10)
            resp.raise_for_status()
            payload_json = resp.json()
            cols = payload_json.get("result", {}).get("collections", [])
            if isinstance(cols, list):
                qdrant_collections = [c.get("name") for c in cols if isinstance(c, dict) and isinstance(c.get("name"), str)]
        except Exception:
            pass
        retrieved = vectordb_top_texts_for_word(
            payload.word,
            limit=payload.limit,
            min_text_length=payload.min_text_length,
            allow_disabled=True,
        )
        if not retrieved:
            raise HTTPException(
                status_code=404,
                detail=(
                    "VectorDB returned no texts. "
                    "Check that QDRANT collection exists and payload text key. "
                    f"Qdrant collections={qdrant_collections or '[]'}; "
                    f"QDRANT_COLLECTION={QDRANT_COLLECTION}; min_text_length={payload.min_text_length}."
                ),
            )
        return {"word": payload.word, "retrieved_texts": retrieved, "vectordb_error": _vectordb_load_error}
    finally:
        _docker_stop_qdrant_if_started(started)


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
@app.post("/getWordPopularityTimeline")
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
    global _llm_load_error
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

    sentiment_lines = [f"- {w}: {sentiment_map.get(w, 0.0):.4f}" for w in words]
    scores = [sentiment_map.get(w, 0.0) for w in words]
    avg_score = sum(scores) / max(len(scores), 1)
    neg_frac = (sum(1 for s in scores if s < 0.0) / max(len(scores), 1)) if scores else 0.0
    min_score = min(scores) if scores else 0.0
    
    user_prompt = (
        "You are a deterministic classifier. Your ONLY job is to output one of two labels: Post or Do not post.\n"
        "You MUST base your decision ONLY on the provided VADER sentiment scores and the computed aggregates.\n"
        "Do NOT use any world knowledge, topic intuition, safety policy, or assumptions about the text beyond the scores.\n"
        "Treat any missing word sentiment as exactly 0.0 (neutral).\n\n"
        "Definitions:\n"
        "- VADER score range: [-1.0, 1.0]; 0.0 is neutral.\n"
        "- avg_score: average of all word scores in the sentence (including 0.0 for unknowns).\n"
        "- neg_frac: fraction of words with score < 0.0.\n\n"
        "Decision rules (apply in order):\n"
        "1) If avg_score <= -0.05 OR neg_frac >= 0.40 OR min_score <= -0.25 => Do not post\n"
        "2) Else => Post\n\n"
        f"Sentence: {payload.sentence}\n"
        f"Computed aggregates: avg_score={avg_score:.4f}, neg_frac={neg_frac:.2f}, min_score={min_score:.4f}\n"
        "Word sentiments:\n"
        + "\n".join(sentiment_lines)
        + "\n\nOutput format (exactly one line):\nRecommendation: <Post/Do not post> - <must mention which rule triggered and the aggregates>"
    )

    llm = get_llm()
    if llm is None:
        if not ALLOW_SENTIMENT_FALLBACK:
            detail = (
                "LLM is unavailable for action recommendation. "
                f"base_url={LLM_BASE_URL} configured_model={LLM_MODEL or '<auto>'} "
                f"error={_llm_load_error or 'unknown'}"
            )
            if _llm_load_error:
                detail = f"{detail} Loader error: {_llm_load_error}"
            raise HTTPException(status_code=503, detail=detail)
        fallback = (
            "Recommendation: Post - overall sentiment is positive or neutral."
            if avg_score >= 0
            else "Recommendation: Do not post - overall sentiment trends negative."
        )
        return {"response": fallback}

    try:
        text_out = llm_chat_completion(
            messages=[{"role": "user", "content": user_prompt}],
            max_tokens=200,
            temperature=0.0,
        )
    except Exception as exc:
        _llm_load_error = f"{type(exc).__name__}: {exc}"
        logger.exception("actionRecommend LLM call failed.")
        if not ALLOW_SENTIMENT_FALLBACK:
            raise HTTPException(
                status_code=503,
                detail=(
                    "LLM request failed in action recommendation. "
                    f"base_url={LLM_BASE_URL} model={_llm_model_in_use or LLM_MODEL or '<auto>'} "
                    f"error={_llm_load_error}"
                ),
            ) from exc
        fallback = (
            deterministic_action_recommendation(avg_score=avg_score, neg_frac=neg_frac, min_score=min_score)
        )
        return {"response": fallback}
    if not text_out.strip():
        return {"response": deterministic_action_recommendation(avg_score=avg_score, neg_frac=neg_frac, min_score=min_score)}
    if "recommendation:" not in text_out.lower():
        return {"response": deterministic_action_recommendation(avg_score=avg_score, neg_frac=neg_frac, min_score=min_score)}
    return {"response": text_out}


@app.post("/why-sentiments")
def why_sentiments(payload: WhySentimentsRequest) -> dict[str, Any]:
    global _llm_load_error
    # 1) Fetch full sentiment series for the word in range.
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
    full_points = [{to_iso(r.time_range): float(r.sentiment_score)} for r in rows]
    if not full_points:
        raise HTTPException(status_code=404, detail="No sentiment data found for this word in the selected window.")

    sampled_points = sample_timeseries(full_points, payload.sample_points)

    # 2) Retrieve top context snippets.
    # If the frontend already provided snippets, we skip Qdrant entirely.
    if payload.retrieved_texts is not None:
        retrieved_texts = payload.retrieved_texts
        used_vectordb = False
    else:
        retrieved_texts = vectordb_top_texts_for_word(payload.word, limit=5)
        used_vectordb = True

    if payload.retrieved_texts is not None and not retrieved_texts:
        raise HTTPException(
            status_code=400,
            detail="retrieved_texts must be a non-empty list (run 'retrieve posts' first).",
        )

    # 3) Ask the LLM for an explanation grounded in the data + retrieved snippets.
    llm = get_llm()
    if llm is None:
        detail = (
            "LLM is unavailable for why-sentiments. "
            f"base_url={LLM_BASE_URL} configured_model={LLM_MODEL or '<auto>'} "
            f"error={_llm_load_error or 'unknown'}"
        )
        if _llm_load_error:
            detail = f"{detail} Loader error: {_llm_load_error}"
        raise HTTPException(status_code=503, detail=detail)

    prompt = (
        f"You are a strict, concise summarizer. Explain the context and sentiment around the word '{payload.word}'.\n\n"
        "STRICT CONSTRAINTS:\n"
        "1. DO NOT output any internal thinking, chain-of-thought, or meta-commentary (e.g., 'Here is an analysis', 'Based on the posts').\n"
        "2. DO NOT use bullet points, lists, or multiple paragraphs.\n"
        "3. Output exactly ONE short, concise paragraph (2-4 sentences maximum).\n"
        "4. Directly state *how* people are using the word in these posts and *why* the sentiment is what it is.\n\n"
        f"Word: {payload.word}\n"
        "Retrieved Posts:\n"
        + "\n".join([f"- {t}" for t in retrieved_texts])
    )

    try:
        text_out = llm_chat_completion(
            messages=[{"role": "user", "content": prompt}],
            max_tokens=350,
            temperature=0.2,
        )
    except Exception as exc:
        _llm_load_error = f"{type(exc).__name__}: {exc}"
        logger.exception("why-sentiments LLM call failed.")
        raise HTTPException(
            status_code=503,
            detail=(
                "LLM request failed in why-sentiments. "
                f"base_url={LLM_BASE_URL} model={_llm_model_in_use or LLM_MODEL or '<auto>'} "
                f"error={_llm_load_error}"
            ),
        ) from exc
    return {
        "word": payload.word,
        "sampled_sentiments": sampled_points,
        "retrieved": retrieved_texts,
        "response": text_out,
        "vectordb_available": bool(retrieved_texts),
        "vectordb_error": _vectordb_load_error if used_vectordb else None,
    }
@app.get("/llmStatus")
def llm_status() -> dict[str, Any]:
    llm = get_llm()
    return {
        "loaded": llm is not None,
        "base_url": LLM_BASE_URL,
        "model": _llm_model_in_use or LLM_MODEL or None,
        "allow_fallback": ALLOW_SENTIMENT_FALLBACK,
        "error": _llm_load_error,
    }


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

