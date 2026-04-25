from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import json
from pathlib import Path
from typing import Dict, List, Any

app = FastAPI()

# This allows your React dashboard (usually on localhost:3000) to talk to this API
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
DASHBOARD_HTML = Path(__file__).resolve().parent / "dashboard.html"

DATASET_FOLDERS: Dict[str, str] = {
    "sentiment_trends": "hourly_series_data",
    "global_word_count": "global_word_count",
    "subreddit_timeseries": "time_series_subreddits",
    "word_scores": "word_scores",
    "graph_edges": "graph_edges",
    "controversial_posts": "controversial_posts",
}


def _read_json_lines_file(file_path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with file_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _load_dataset(folder_name: str) -> List[Dict[str, Any]]:
    target_dir = OUTPUT_DIR / folder_name
    if not target_dir.exists():
        return []

    json_files = sorted(target_dir.glob("part-*.json"))
    if not json_files:
        json_files = sorted(target_dir.glob("*.jsonl"))

    rows: List[Dict[str, Any]] = []
    for file_path in json_files:
        rows.extend(_read_json_lines_file(file_path))
    return rows


def _dataset_status() -> Dict[str, Any]:
    status: Dict[str, Any] = {}
    for dataset_key, folder_name in DATASET_FOLDERS.items():
        rows = _load_dataset(folder_name)
        status[dataset_key] = {
            "rows": len(rows),
            "ready": len(rows) > 0,
            "folder": folder_name,
        }
    return status


@app.get("/")
def get_dashboard():
    if not DASHBOARD_HTML.exists():
        raise HTTPException(status_code=404, detail="Dashboard file not found.")
    return FileResponse(DASHBOARD_HTML)

@app.get("/api/sentiment-trends")
def get_trends():
    return _load_dataset(DATASET_FOLDERS["sentiment_trends"])


@app.get("/api/global-word-count")
def get_global_word_count():
    return _load_dataset(DATASET_FOLDERS["global_word_count"])


@app.get("/api/subreddit-timeseries")
def get_subreddit_timeseries():
    return _load_dataset(DATASET_FOLDERS["subreddit_timeseries"])


@app.get("/api/word-scores")
def get_word_scores():
    return _load_dataset(DATASET_FOLDERS["word_scores"])


@app.get("/api/graph-edges")
def get_graph_edges():
    return _load_dataset(DATASET_FOLDERS["graph_edges"])


@app.get("/api/controversial-posts")
def get_controversial_posts():
    return _load_dataset(DATASET_FOLDERS["controversial_posts"])


@app.get("/api/dashboard-summary")
def get_dashboard_summary():
    return {"datasets": _dataset_status()}

@app.get("/api/chat")
async def chat_with_expert(query: str):
    from rag_reddit import ask_reddit_expert
    # This calls your ChromaDB + Gemini logic
    answer = ask_reddit_expert(query)
    return {"answer": answer}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)