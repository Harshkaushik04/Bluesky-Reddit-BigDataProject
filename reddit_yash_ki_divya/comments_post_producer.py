import requests
import time
import json
import os
from collections import deque

# 1. Configuration
BRONZE_DIR = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project_extra\reddit\reddit_data2"
POSTS_DIR = os.path.join(BRONZE_DIR, "posts")
COMMENTS_DIR = os.path.join(BRONZE_DIR, "comments")

REDDIT_POSTS_URL = "https://www.reddit.com/r/all/new.json?limit=100"
REDDIT_COMMENTS_URL = "https://www.reddit.com/r/all/comments.json?limit=97"
HEADERS = {'User-Agent': 'academic:IIT_Ropar_Project:v1.0 (by /u/Training-Chemist-568)'}

# Ensure both sub-directories exist before starting
os.makedirs(POSTS_DIR, exist_ok=True)
os.makedirs(COMMENTS_DIR, exist_ok=True)

# 2. Filename generator
def get_current_filename(data_type):
    now = time.localtime()
    date_str = time.strftime("%Y-%m-%d", now)
    hour_str = time.strftime("%H", now)
    
    # Route to the correct folder
    if data_type == "posts":
        return os.path.join(POSTS_DIR, f"reddit_posts_{date_str}_{hour_str}.jsonl")
    else:
        return os.path.join(COMMENTS_DIR, f"reddit_comments_{date_str}_{hour_str}.jsonl")

def fetch_data():
    print("Starting data collection for Posts AND Comments. Press Ctrl+C to stop.")
    
    seen_post_ids = deque(maxlen=2000)
    seen_comment_ids = deque(maxlen=2000)
    
    while True:
        try:
            # ==========================================
            # 1. FETCH AND SAVE POSTS
            # ==========================================
            response_posts = requests.get(REDDIT_POSTS_URL, headers=HEADERS)
            if response_posts.status_code == 200:
                data = response_posts.json()
                posts = data['data']['children']
                filename = get_current_filename("posts")
                new_posts_count = 0
                
                with open(filename, 'a', encoding='utf-8') as file:
                    for post in reversed(posts):
                        post_id = post['data']['name']
                        if post_id not in seen_post_ids:
                            file.write(json.dumps(post['data']) + '\n')
                            seen_post_ids.append(post_id)
                            new_posts_count += 1
                
                print(f"[{time.strftime('%H:%M:%S')}] Saved {new_posts_count} new posts into the 'posts' folder.")
                
            elif response_posts.status_code == 429:
                print("Reddit rate limit reached for posts. Waiting 60 seconds...")
                time.sleep(60)
                continue

            # Pause between calls to respect Reddit API rate limits
            time.sleep(2)

            # ==========================================
            # 2. FETCH AND SAVE COMMENTS
            # ==========================================
            response_comments = requests.get(REDDIT_COMMENTS_URL, headers=HEADERS)
            if response_comments.status_code == 200:
                data = response_comments.json()
                comments = data['data']['children']
                filename = get_current_filename("comments")
                new_comments_count = 0
                
                with open(filename, 'a', encoding='utf-8') as file:
                    for comment in reversed(comments):
                        comment_id = comment['data']['name']
                        if comment_id not in seen_comment_ids:
                            file.write(json.dumps(comment['data']) + '\n')
                            seen_comment_ids.append(comment_id)
                            new_comments_count += 1
                
                print(f"[{time.strftime('%H:%M:%S')}] Saved {new_comments_count} new comments into the 'comments' folder.")

            elif response_comments.status_code == 429:
                print("Reddit rate limit reached for comments. Waiting 60 seconds...")
                time.sleep(60)
                continue

        except Exception as error:
            print(f"Connection Error: {error}")
            
        # Wait before starting the next cycle
        time.sleep(5)

if __name__ == "__main__":
    fetch_data()