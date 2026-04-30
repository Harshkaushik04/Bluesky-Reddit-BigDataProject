import json
import glob

def get_keys(path):
    keys = set()
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i > 100: break
                try:
                    data = json.loads(line)
                    keys.update(data.keys())
                except:
                    pass
    except Exception as e:
        print(f"Error reading {path}: {e}")
    return keys

old_posts = get_keys('d:/Documents_D/HOMEWORK/6th_sem/Big_Data_AI528/project/Bluesky-Reddit-BigDataProject/reddit_yash_ki_divya/data/posts/2025r_politics_posts.jsonl')
new_posts = get_keys('d:/Documents_D/HOMEWORK/6th_sem/Big_Data_AI528/project/Bluesky-Reddit-BigDataProject/reddit_yash_ki_divya/data/posts_live/reddit_posts_2026-04-27_08.jsonl')
new_comments = get_keys('d:/Documents_D/HOMEWORK/6th_sem/Big_Data_AI528/project/Bluesky-Reddit-BigDataProject/reddit_yash_ki_divya/data/comments_live/reddit_comments_2026-04-27_08.jsonl')

print("Old Posts Keys:", len(old_posts))
print("New Posts Keys:", len(new_posts))
print("New Comments Keys:", len(new_comments))

print("\nMissing in New Posts (compared to old posts):")
print(old_posts - new_posts)

print("\nNew in New Posts (compared to old posts):")
print(new_posts - old_posts)

print("\nMissing in New Comments (compared to old posts):")
print(old_posts - new_comments)
