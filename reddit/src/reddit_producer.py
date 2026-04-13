import requests
import time
import json
import os
from collections import deque

# 1. Configuration
BRONZE_DIR = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\reddit_data"
REDDIT_URL = "https://www.reddit.com/r/all/new.json?limit=100"
HEADERS = {'User-Agent': 'academic:IIT_Ropar_Project:v1.0 (by /u/Training-Chemist-568)'}

if not os.path.exists(BRONZE_DIR):
    os.makedirs(BRONZE_DIR, exist_ok=True)

def get_current_filename():
    now = time.localtime()
    date_str = time.strftime("%Y-%m-%d", now)
    hour_str = time.strftime("%H", now)
    return os.path.join(BRONZE_DIR, f"reddit_{date_str}_{hour_str}.jsonl")

def fetch_data():
    print("Starting data collection. Press Ctrl+C to stop.")
    # seen_ids manages its own size; no manual cleanup needed
    seen_ids = deque(maxlen=2000)
    
    while True:
        try:
            response = requests.get(REDDIT_URL, headers=HEADERS)
            
            if response.status_code == 200:
                data = response.json()
                posts = data['data']['children']
                filename = get_current_filename()
                new_posts_count = 0
                
                with open(filename, 'a', encoding='utf-8') as file:
                    # reversed() ensures the oldest posts of the batch 
                    # are written to the file first
                    for post in reversed(posts):
                        post_id = post['data']['name']
                        
                        if post_id not in seen_ids:
                            file.write(json.dumps(post['data']) + '\n')
                            seen_ids.append(post_id)
                            new_posts_count += 1
                
                print(f"[{time.strftime('%H:%M:%S')}] Saved {new_posts_count} new posts to disk.")
                    
            elif response.status_code == 429:
                print("Reddit rate limit reached. Waiting 60 seconds...")
                time.sleep(60)
            else:
                print(f"Error: {response.status_code}")
                
        except Exception as error:
            print(f"Connection Error: {error}")
            
        time.sleep(5)

if __name__ == "__main__":
    fetch_data()