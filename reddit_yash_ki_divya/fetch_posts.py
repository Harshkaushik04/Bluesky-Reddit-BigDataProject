import requests
import pandas as pd
import time

HEADERS = {
    "User-Agent": "python:bigdata.project:v1.0 (by /u/yourusername)"
}


def safe_request(url):
    for i in range(3):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            return response
        except Exception:
            print("⚠️ Network error, retrying...", i + 1)
            time.sleep(3)
    return None


def fetch_posts(limit=100):
    url = f"https://api.reddit.com/r/all/new?limit={limit}"

    try:
        response = safe_request(url)
        if response is None:
            return pd.DataFrame()

        print("📡 Posts r/all/new → Status:", response.status_code)

        if response.status_code != 200:
            return pd.DataFrame()

        data = response.json()

    except Exception as e:
        print("❌ Error fetching posts:", e)
        return pd.DataFrame()

    posts = []

    for item in data["data"]["children"]:
        post = item["data"]

        posts.append({
            "post_id": post.get("id"),
            "title": post.get("title"),
            "post_score": post.get("score"),
            "post_time": post.get("created_utc"),
            "subreddit": post.get("subreddit"),
            "author": post.get("author"),
            "url": post.get("url"),
            "num_comments": post.get("num_comments")
        })

    return pd.DataFrame(posts)