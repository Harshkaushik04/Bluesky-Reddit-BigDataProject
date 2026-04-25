import requests
import pandas as pd
import time
import os
from datetime import datetime
from collections import deque

# 🛑 IMPORTANT: Change 'yourusername' to your actual Reddit username!
headers = {
    "User-Agent": "python:bigdata.project:v1.0 (by /u/Training-Chemist-568)"
}

def safe_request(url):
    for i in range(3):  # retry 3 times
        try:
            response = requests.get(url, headers=headers, timeout=10)
            return response
        except Exception as e:
            print("⚠️ Network error, retrying...", i+1)
            time.sleep(3)
    return None

# =========================
# 🔹 FETCH POSTS (r/all/new)
# =========================
def fetch_posts():
    # r/all/new gets the latest posts from EVERY public subreddit
    url = "https://api.reddit.com/r/all/new?limit=100"
    res = safe_request(url)

    if res is None:
        return []

    print("Status:", res.status_code)

    if res.status_code != 200:
        return []

    # 🔹 The "Shock Absorber" prevents HTML soft-block crashes
    try:
        data = res.json()["data"]["children"]
    except requests.exceptions.JSONDecodeError:
        print("⚠️ Reddit returned an HTML page instead of JSON. Skipping this batch...")
        return []
    except Exception as e:
        print(f"⚠️ Unexpected error parsing data: {e}")
        return []

    posts = []
    for item in data:
        post = item["data"]
        
        # Using .get() for safety in case a field is missing
        posts.append({
            "post_id": post.get("id"),
            "title": post.get("title"),
            "post_score": post.get("score"),
            "post_time": post.get("created_utc"),
            "subreddit": post.get("subreddit"),
            "author": post.get("author", "[deleted]"),
            "url": post.get("url", ""),
            "num_comments": post.get("num_comments", 0)
        })

    return posts

# =========================
# 🔁 MAIN LOOP
# =========================
runs = int(input("👉 Enter how many times to run pipeline: "))

# Increased memory bank to 2000 to be extra safe against duplicates
recent_post_ids = deque(maxlen=2000) 

for run in range(runs):
    print(f"\n================ RUN {run+1} / {runs} ================\n")
    print("📥 Fetching newest posts from r/all...")
    
    all_posts = fetch_posts()
    all_data = []

    # Keep only posts not present in the rolling cache
    for post in all_posts:
        post_id = post["post_id"]
        if post_id not in recent_post_ids:
            all_data.append(post)
            recent_post_ids.append(post_id)

    # 🔹 Save after each run
    os.makedirs("../data/raw", exist_ok=True)
    df = pd.DataFrame(all_data)

    if not df.empty:
        # Final safety check for duplicates within this specific batch
        df.drop_duplicates(subset=["post_id"], inplace=True)

        # Create hourly batch file name
        hour_stamp = datetime.utcnow().strftime("%Y%m%d_%H")
        output_path = f"../data/raw/reddit_data_{hour_stamp}.jsonl"

        df.to_json(output_path,
                   orient="records",
                   lines=True,
                   mode="a")

        print(f"✅ Run {run+1} completed!")
        print(f"Records added: {len(df)}")
    else:
        print(f"⚠️ Run {run+1} yielded no new data. (Might be fetching too fast)")

    # 🔹 wait before next run (VERY IMPORTANT)
    if run < runs - 1:
        print("⏳ Waiting 10 seconds before next run...\n")
        time.sleep(10) # 10 seconds gives Reddit time to populate new posts