import sqlite3
import torch
import uuid
import time
from pathlib import Path
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client import models

# --- 1. Configuration ---
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "reddit_dashboard.db"
COLLECTION_NAME = "reddit_posts_and_comments"
BATCH_SIZE = 100

NAMESPACE_REDDIT = uuid.uuid5(uuid.NAMESPACE_URL, "reddit.com")

# --- 2. Initialize Qdrant Client ---
print("Connecting to Qdrant...")
client = QdrantClient(url="http://localhost:6333", timeout=60.0)

try:
    client.get_collection(collection_name=COLLECTION_NAME)
    print(f"Collection '{COLLECTION_NAME}' already exists. Dropping it to start fresh.")
    client.delete_collection(collection_name=COLLECTION_NAME)
except Exception:
    pass

client.create_collection(
    collection_name=COLLECTION_NAME,
    vectors_config=models.VectorParams(
        size=384, 
        distance=models.Distance.COSINE,
        on_disk=True 
    ),
    quantization_config=models.ScalarQuantization(
        scalar=models.ScalarQuantizationConfig(
            type=models.ScalarType.INT8,
            always_ram=False 
        )
    )
)

# --- 3. Initialize the Embedding Model ---
print("Loading model...")
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device: {device}")
model = SentenceTransformer('all-MiniLM-L6-v2', device=device)

# --- 4. Fetch Data & Upsert ---
print("Starting ingestion from SQLite...")
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# We will fetch only post titles (not comments, to keep it fast)
cursor.execute("""
    SELECT 'post' as type, title as text, post_id as id, score, created_date 
    FROM reddit_post_facts 
    WHERE title IS NOT NULL AND title != '' AND post_type != 'comment'
""")

rows = cursor.fetchall()
conn.close()

total_processed = 0
start_time = time.time()

for i in range(0, len(rows), BATCH_SIZE):
    batch = rows[i:i+BATCH_SIZE]
    
    texts = []
    types = []
    ids = []
    scores = []
    times = []
    
    for row in batch:
        r_type, r_text, r_id, r_score, r_time = row
        texts.append(str(r_text))
        types.append(r_type)
        ids.append(str(r_id))
        scores.append(r_score)
        times.append(r_time)
        
    if not texts:
        continue
        
    embeddings = model.encode(texts, batch_size=256, normalize_embeddings=True, show_progress_bar=False)
    
    points = []
    for j in range(len(texts)):
        qdrant_uuid = str(uuid.uuid5(NAMESPACE_REDDIT, f"{types[j]}_{ids[j]}"))
        points.append(
            models.PointStruct(
                id=qdrant_uuid, 
                vector=embeddings[j].tolist(),
                payload={
                    "text": texts[j],
                    "type": types[j],
                    "id": ids[j],
                    "score": scores[j],
                    "time_iso": times[j]
                }
            )
        )
        
    client.upsert(
        collection_name=COLLECTION_NAME,
        points=points
    )
    
    total_processed += len(texts)
    if (i // BATCH_SIZE) % 5 == 0:
        elapsed = time.time() - start_time
        rate = total_processed / elapsed if elapsed > 0 else 0
        print(f"Upserted {total_processed:,} / {len(rows):,} records... ({rate:.2f} rows/sec)")

elapsed = time.time() - start_time
print(f"Ingestion completely finished! Total: {total_processed:,} records in {elapsed:.2f} seconds.")
