import requests
import pandas as pd
import time

def fetch_posts(subreddit="news", limit=50):

    url = f"https://api.reddit.com/r/{subreddit}/top?t=week&limit={limit}"

    headers = {
        "User-Agent": "python:bigdata.project:v1.0 (by /u/temporary_user)"
    }

    try:
        response = safe_requests.get(url)
        if responce is None:
            return []

        print(f"📡 Posts {subreddit} → Status:", response.status_code)

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
            "subreddit": subreddit,
            "post_time": post.get("created_utc")
        })

    return pd.DataFrame(posts)