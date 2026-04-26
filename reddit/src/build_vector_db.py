import json
import chromadb
import os
import glob

def setup_rag_database():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_folder_path = os.path.join(script_dir, "..", "reddit_data")
    output_db_path = os.path.join(script_dir, "..", "output", "vector_db")

    os.makedirs(output_db_path, exist_ok=True)

    print(f"1. Spinning up ChromaDB in 'output/vector_db'...")
    client = chromadb.PersistentClient(path=output_db_path)
    collection = client.get_or_create_collection(name="reddit_knowledge")

    documents, metadatas, ids = [], [], []

    print(f"2. Reading your Reddit data from '{input_folder_path}'...")
    search_pattern = os.path.join(input_folder_path, "*.jsonl")
    file_list = glob.glob(search_pattern)

    if not file_list:
        print(f"❌ Error: Could not find any .jsonl files!")
        return None

    global_index = 0
    for file_path in file_list:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    title = data.get("title", "")
                    if not title: continue
                        
                    text = f"Post Title: {title}\nEngagement: {data.get('score', 0)} upvotes, {data.get('num_comments', 0)} comments."
                    documents.append(text)
                    metadatas.append({"subreddit": data.get("subreddit", "Unknown"), "score": data.get("score", 0)})
                    ids.append(f"post_{global_index}")
                    global_index += 1
                except json.JSONDecodeError:
                    continue

    if not documents:
        print("❌ Error: No valid data extracted.")
        return None

    # --- PROGRESS TRACKING LOGIC ---
    total_posts = len(documents)
    batch_size = 100  # Process 100 posts at a time
    print(f"3. Starting vectorization of {total_posts} posts...")

    for i in range(0, total_posts, batch_size):
        batch_docs = documents[i : i + batch_size]
        batch_metas = metadatas[i : i + batch_size]
        batch_ids = ids[i : i + batch_size]
        
        collection.add(documents=batch_docs, metadatas=batch_metas, ids=batch_ids)
        
        percent_done = min(100, round(((i + len(batch_docs)) / total_posts) * 100, 2))
        print(f"🚀 Progress: {percent_done}% complete ({i + len(batch_docs)}/{total_posts} posts vectorized)")

    print(f"✅ Database successfully built and saved!\n")
    return collection

my_collection = setup_rag_database()