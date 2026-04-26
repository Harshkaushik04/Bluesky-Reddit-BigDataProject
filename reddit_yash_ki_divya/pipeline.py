import requests
import pandas as pd
import time
import os
from datetime import datetime
from collections import deque
import msvcrt

headers = {
    "User-Agent": "python:bigdata.project:v1.0 (by /u/Training-Chemist-568)"
}

request_stats = {
    "total_requests_sent": 0,
    "requests_this_run": 0
}


def safe_request(url):
    for i in range(3):  # retry 3 times
        try:
            request_stats["total_requests_sent"] += 1
            request_stats["requests_this_run"] += 1
            response = requests.get(url, headers=headers, timeout=10)
            return response
        except Exception as e:
            print("⚠️ Network error, retrying...", i+1)
            time.sleep(3)
    return None


# =========================
# 🔹 FETCH POSTS
# =========================
def fetch_posts():
    url = "https://api.reddit.com/r/all/new?limit=100"
    res = safe_request(url)

    if res is None:
        return []

    print("Status:", res.status_code)

    if res.status_code != 200:
        print("⚠️ Non-200 response received, skipping this cycle.")
        return []

    content_type = res.headers.get("Content-Type", "")
    if "application/json" not in content_type.lower():
        print("⚠️ Non-JSON response received, skipping this cycle.")
        return []

    try:
        payload = res.json()
    except ValueError:
        print("⚠️ Failed to decode JSON response, skipping this cycle.")
        return []

    if "data" not in payload or "children" not in payload["data"]:
        print("⚠️ Unexpected response structure, skipping this cycle.")
        return []

    data = payload["data"]["children"]

    posts = []
    for item in data:
        post = item["data"]
        posts.append({
            "post_id": post["id"],
            "title": post["title"],
            "post_score": post["score"],
            "post_time": post["created_utc"],
            "subreddit": post["subreddit"],
            "author": post.get("author"),
            "url": post.get("url"),
            "num_comments": post.get("num_comments")
        })

    return posts


# =========================
# 🔁 MAIN LOOP
# =========================
recent_post_ids = deque(maxlen=1000)
run = 0
start_time = time.time()
recommended_max_per_min = 60  # Conservative recommendation for Reddit API usage.

print("▶ Continuous fetch started. Press Ctrl+C or press 'q' to stop.")
print("ℹ Fetch interval: 5 seconds\n")
print(f"ℹ Recommended max request rate: ~{recommended_max_per_min} requests/minute\n")

try:
    while True:
        run += 1
        request_stats["requests_this_run"] = 0
        print(f"\n================ RUN {run} ================\n")
        print("📥 Fetching newest posts from r/all...")
        all_posts = fetch_posts()
        all_data = []

        # Keep only posts not present in the rolling last 1000 IDs cache.
        for post in all_posts:
            post_id = post["post_id"]
            if post_id not in recent_post_ids:
                all_data.append(post)
                recent_post_ids.append(post_id)

        # 🔹 Save after each run
        os.makedirs("../data/raw", exist_ok=True)
        df = pd.DataFrame(all_data)

        # Remove duplicates by post id within this batch
        if not df.empty:
            df.drop_duplicates(subset=["post_id"], inplace=True)

        hour_stamp = datetime.utcnow().strftime("%Y%m%d_%H")
        output_path = f"../data/raw/reddit_data_{hour_stamp}.jsonl"

        if not df.empty:
            df.to_json(
                output_path,
                orient="records",
                lines=True,
                mode="a"
            )

        print(f"✅ Run {run} completed!")
        print("Records added:", len(df))
        print("Requests this run:", request_stats["requests_this_run"])
        print("Total requests sent:", request_stats["total_requests_sent"])

        elapsed_minutes = max((time.time() - start_time) / 60, 1e-6)
        avg_req_per_min = request_stats["total_requests_sent"] / elapsed_minutes
        print(f"Average request rate: {avg_req_per_min:.2f} requests/minute")
        print("⏳ Waiting 5 seconds... (press 'q' to stop)")

        # Check keypress during 5-second wait without blocking.
        should_quit = False
        for _ in range(50):
            if msvcrt.kbhit():
                key = msvcrt.getwch().lower()
                if key == "q":
                    should_quit = True
                    break
            time.sleep(0.1)

        if should_quit:
            print("🛑 Stopped by user keypress 'q'.")
            break

except KeyboardInterrupt:
    print("\n🛑 Stopped by user (Ctrl+C).")