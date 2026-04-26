import requests
import json
import os
import time

# 1. Setup the Target URL and Headers
# 't=week' gets the most controversial posts from the last 7 days. You can change this to 'day' or 'month'.
# 'limit=100' is the maximum Reddit allows per single unauthenticated request.
subreddit = "all"
url = f"https://www.reddit.com/r/{subreddit}/controversial.json?t=week&limit=100"

# CRITICAL: Reddit blocks default Python requests. You MUST fake a web browser User-Agent.
headers = {
    'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 AI528_Project_Bot'
}

print(f"Fetching controversial posts from r/{subreddit}...")

# 2. Make the HTTP Request
response = requests.get(url, headers=headers)

# 3. Check for Success
if response.status_code != 200:
    print(f"Failed to fetch data! HTTP Status Code: {response.status_code}")
    print("If you see 429, you are being rate-limited. Wait 10 minutes.")
else:
    # 4. Parse the massive JSON response
    raw_data = response.json()
    posts = raw_data['data']['children']
    
    print(f"Successfully downloaded {len(posts)} controversial posts.")
    
    # 5. Define where to save the Bronze Data
    # We save this in your reddit_data folder so your PySpark scripts will automatically read it!
    output_dir = r"D:\Documents_D\HOMEWORK\6th_sem\Big_Data_AI528\project\Bluesky-Reddit-BigDataProject\reddit\output\controversial_posts"
    os.makedirs(output_dir, exist_ok=True)
    
    # Use a timestamp in the filename so you don't overwrite previous scrapes
    timestamp = int(time.time())
    output_file = os.path.join(output_dir, f"controversial_{timestamp}.jsonl")
    
    print("Extracting clean data and saving to JSONL...")
    
    # 6. Extract only what PySpark needs and write as JSON Lines (JSONL)
    with open(output_file, 'w', encoding='utf-8') as f:
        for post in posts:
            post_data = post['data']
            
            # Create a clean dictionary matching your existing schema
            clean_post = {
                "title": post_data.get('title', ''),
                "subreddit": post_data.get('subreddit', ''),
                "created_utc": post_data.get('created_utc', 0),
                "score": post_data.get('score', 0),
                "num_comments": post_data.get('num_comments', 0),
                "is_controversial_scrape": True # A flag to help you filter later if needed
            }
            
            # Write one JSON object per line
            f.write(json.dumps(clean_post) + '\n')
            
    print(f"\n✅ Success! Data saved to: {output_file}")