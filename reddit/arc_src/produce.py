from curl_cffi import requests # IMPORTANT: We use curl_cffi instead of standard requests
import json
import time
from datetime import datetime, timedelta
import os

def fetch_arctic_shift_weekly(subreddit, year=2025):
    start_date = datetime(year, 1, 1)
    end_of_year = datetime(year, 12, 31)
    current_start = start_date
    
    os.makedirs("../arc_reddit_data", exist_ok=True)
    output_file = f"../arc_reddit_data/arctic_{subreddit}_{year}.jsonl"
    
    print(f"🚀 Starting Arctic Shift collection for {year}...")
    
    with open(output_file, "w", encoding='utf-8') as f:
        while current_start < end_of_year:
            current_end = current_start + timedelta(days=7)
            if current_end > end_of_year:
                current_end = end_of_year
                
            str_start = current_start.strftime("%Y-%m-%d")
            str_end = current_end.strftime("%Y-%m-%d")
            
            print(f"📅 Fetching week: {str_start} to {str_end}")
            
            # Arctic Shift API endpoint
            url = f"https://arctic-shift.photon-reddit.com/api/posts/search"
            
            params = {
                "subreddit": subreddit,
                "after": str_start,
                "before": str_end,
                "limit": 50
            }
            
            try:
                # THE BYPASS: Impersonating Chrome to avoid the 403 Anti-Bot block
                response = requests.get(url, params=params, impersonate="chrome110")
                
                if response.status_code == 200:
                    data = response.json()
                    posts = data.get('data', [])
                    
                    for post in posts:
                        f.write(json.dumps({
                            "title": post.get("title"),
                            "text": post.get("body", post.get("selftext", "")),
                            "subreddit": post.get("subreddit"),
                            "score": post.get("score", 0),
                            "created_utc": post.get("created_utc"),
                            "week_start": str_start 
                        }) + "\n")
                        
                    print(f"   ✅ Saved {len(posts)} posts.")
                else:
                    print(f"   ❌ Failed: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"   ⚠️ Network Error: {e}")
                
            # Play it safe with rate limits
            time.sleep(4) 
            current_start = current_end

    print(f"\n🎉 Done! All data saved to {output_file}")

if __name__ == "__main__":
    fetch_arctic_shift_weekly("MachineLearning", year=2025)