import requests
import time
import json
import os
from collections import deque
from pathlib import Path
from dotenv import load_dotenv

# Load central .env
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# 1. Configuration
BRONZE_DIR = Path(os.getenv("REDDIT_DATA_DIR", Path(__file__).resolve().parent / "data")).resolve()
POSTS_DIR = os.path.join(BRONZE_DIR, "posts")
COMMENTS_DIR = os.path.join(BRONZE_DIR, "comments")
POSTS_LIVE_DIR = os.path.join(BRONZE_DIR, "posts_live")
COMMENTS_LIVE_DIR = os.path.join(BRONZE_DIR, "comments_live")

REDDIT_POSTS_URL = "https://www.reddit.com/r/politics/new.json?limit=100"
REDDIT_COMMENTS_URL = "https://www.reddit.com/r/politics/comments.json?limit=97"
HEADERS = {'User-Agent': 'academic:IIT_Ropar_Project:v1.0 (by /u/Training-Chemist-568)'}

# Ensure sub-directories exist
os.makedirs(POSTS_DIR, exist_ok=True)
os.makedirs(COMMENTS_DIR, exist_ok=True)
os.makedirs(POSTS_LIVE_DIR, exist_ok=True)
os.makedirs(COMMENTS_LIVE_DIR, exist_ok=True)


def fetch_data():
    print("Starting data collection for r/politics Posts & Comments. Press Ctrl+C to stop.")

    seen_post_ids = deque(maxlen=5000)
    seen_comment_ids = deque(maxlen=5000)
    fetch_count = 0

    while True:
        try:
            fetch_count += 1
            epoch = int(time.time())

            # ==========================================
            # 1. FETCH AND SAVE POSTS
            # ==========================================
            response_posts = requests.get(REDDIT_POSTS_URL, headers=HEADERS)
            if response_posts.status_code == 200:
                data = response_posts.json()
                posts = data['data']['children']
                new_posts = []

                for post in reversed(posts):
                    post_id = post['data']['name']
                    if post_id not in seen_post_ids:
                        new_posts.append(post['data'])
                        seen_post_ids.append(post_id)

                if new_posts:
                    # Atomic write: .tmp_ prefix is ignored by Spark, then rename
                    tmp = os.path.join(POSTS_LIVE_DIR, f".tmp_posts_{epoch}_{fetch_count}.jsonl")
                    final = os.path.join(POSTS_LIVE_DIR, f"reddit_posts_{epoch}_{fetch_count}.jsonl")
                    with open(tmp, 'w', encoding='utf-8') as f:
                        for p in new_posts:
                            f.write(json.dumps(p) + '\n')
                    os.rename(tmp, final)

                print(f"[{time.strftime('%H:%M:%S')}] Saved {len(new_posts)} new posts.")

            elif response_posts.status_code == 429:
                print("Rate limit for posts. Waiting 60s...")
                time.sleep(60)
                continue

            time.sleep(5)

            # ==========================================
            # 2. FETCH AND SAVE COMMENTS
            # ==========================================
            response_comments = requests.get(REDDIT_COMMENTS_URL, headers=HEADERS)
            if response_comments.status_code == 200:
                data = response_comments.json()
                comments = data['data']['children']
                new_comments = []

                for comment in reversed(comments):
                    comment_id = comment['data']['name']
                    if comment_id not in seen_comment_ids:
                        new_comments.append(comment['data'])
                        seen_comment_ids.append(comment_id)

                if new_comments:
                    tmp = os.path.join(COMMENTS_LIVE_DIR, f".tmp_comments_{epoch}_{fetch_count}.jsonl")
                    final = os.path.join(COMMENTS_LIVE_DIR, f"reddit_comments_{epoch}_{fetch_count}.jsonl")
                    with open(tmp, 'w', encoding='utf-8') as f:
                        for c in new_comments:
                            f.write(json.dumps(c) + '\n')
                    os.rename(tmp, final)

                print(f"[{time.strftime('%H:%M:%S')}] Saved {len(new_comments)} new comments.")

            elif response_comments.status_code == 429:
                print("Rate limit for comments. Waiting 60s...")
                time.sleep(60)
                continue

        except Exception as error:
            print(f"Connection Error: {error}")

        # Wait before next cycle
        time.sleep(10)


if __name__ == "__main__":
    fetch_data()