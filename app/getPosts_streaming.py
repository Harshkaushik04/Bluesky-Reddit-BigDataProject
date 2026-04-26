import json
import time
from pathlib import Path
from typing import Iterable

import requests

BASE = Path("D:/Bluesky-Reddit-BigDataProject/Bluesky_data")
TRACKING_FILE = BASE / "silver/getPosts/processed_root_uris.txt"
RESULTS_FILE = BASE / "silver/getPosts/posts_results.jsonl"
STREAMING_FILE = BASE / "streaming/getposts/posts_stream.jsonl"
FIREHOSE_DIR = BASE / "streaming/firehose"
BSKY_API_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"


def ensure_dirs():
    TRACKING_FILE.parent.mkdir(parents=True, exist_ok=True)
    STREAMING_FILE.parent.mkdir(parents=True, exist_ok=True)
    FIREHOSE_DIR.mkdir(parents=True, exist_ok=True)
    TRACKING_FILE.touch(exist_ok=True)
    RESULTS_FILE.touch(exist_ok=True)
    STREAMING_FILE.touch(exist_ok=True)


def load_processed() -> set[str]:
    with TRACKING_FILE.open("r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def iter_new_uris(processed: set[str]) -> Iterable[str]:
    for file_path in sorted(FIREHOSE_DIR.glob("*.jsonl")):
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                uri = payload.get("commit", {}).get("record", {}).get("reply", {}).get("root", {}).get("uri")
                if uri and uri not in processed:
                    yield uri


def fetch_posts(uris: list[str]) -> list[dict]:
    if not uris:
        return []
    params = [("uris", u) for u in uris]
    response = requests.get(BSKY_API_URL, params=params, timeout=20)
    response.raise_for_status()
    return response.json().get("posts", [])


def append_jsonl(path: Path, records: Iterable[dict]):
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()


def append_lines(path: Path, lines: Iterable[str]):
    with path.open("a", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
        f.flush()


def run_forever():
    ensure_dirs()
    while True:
        processed = load_processed()
        new_uris = list(dict.fromkeys(iter_new_uris(processed)))
        if not new_uris:
            print("[getPosts] No new URIs found, sleeping...")
            time.sleep(10)
            continue

        chunk_size = 25
        for i in range(0, len(new_uris), chunk_size):
            chunk = new_uris[i : i + chunk_size]
            try:
                posts = fetch_posts(chunk)
                append_jsonl(RESULTS_FILE, posts)
                append_jsonl(STREAMING_FILE, posts)
                append_lines(TRACKING_FILE, chunk)
                print(f"[getPosts] Saved {len(posts)} posts from {len(chunk)} URIs.")
            except Exception as exc:
                print(f"[getPosts] Failed chunk: {exc}")
        time.sleep(10)


if __name__ == "__main__":
    run_forever()

