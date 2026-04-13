import torch
import pyarrow.dataset as ds
import uuid
import time
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client import models

# --- 1. Configuration ---
PARQUET_DIR = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/silver/readyForSentimentAnalysis/"
COLLECTION_NAME = "all_posts_and_comments"
BATCH_SIZE = 2000  

# A namespace for our deterministic UUID generation
NAMESPACE_BLUESKY = uuid.uuid5(uuid.NAMESPACE_URL, "bluesky.network")

# --- 2. Initialize Qdrant Client ---
print("Connecting to Qdrant...")
client = QdrantClient(url="http://localhost:6333")

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
print("Loading model to RTX 4060...")
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"device:{device}")
model = SentenceTransformer('all-MiniLM-L6-v2', device=device)

# --- 4. Stream Parquet Directory & Upsert ---
print("Starting stream ingestion from directory...")
dataset = ds.dataset(PARQUET_DIR, format="parquet")
total_processed = 0
start_time = time.time()
j=0
for batch in dataset.to_batches(batch_size=BATCH_SIZE):
    batch_dict = batch.to_pydict()
    
    # Safely extract columns row-by-row to handle nested structs and nulls
    texts = []
    dids = []
    rkeys = []
    cids = []
    times = []
    
    commits = batch_dict.get('commit', [])
    raw_dids = batch_dict.get('did', [])
    raw_times = batch_dict.get('time_us', [])
    
    for i in range(len(commits)):
        commit = commits[i]
        
        # Ensure the commit isn't null/None
        if not commit or not isinstance(commit, dict):
            continue
            
        # Safely drill down into the nested dictionaries
        record = commit.get('record')
        text = record.get('text') if (record and isinstance(record, dict)) else None
        cid = commit.get('cid')
        rkey = commit.get('rkey')
        
        # Only keep the row if it has the minimum required data (prevents UUID crashes)
        if text and cid:
            texts.append(str(text))
            cids.append(str(cid))
            rkeys.append(str(rkey) if rkey else "")
            dids.append(str(raw_dids[i]) if i < len(raw_dids) else "")
            times.append(raw_times[i] if i < len(raw_times) else 0)
        j+=1
        # --- Custom Progress Logger ---
        if j % 50000 == 0:
            elapsed = time.time() - start_time
            print(f"Upserted {total_processed:,} records... ({total_processed / elapsed:.2f} rows/sec)")

    # If the entire batch was empty/null, skip to the next batch
    if not texts:
        continue
    
    # Generate embeddings on the GPU for the valid rows
    embeddings = model.encode(texts,batch_size=1024, normalize_embeddings=True, show_progress_bar=False)
    
    points = []
    for i in range(len(texts)):
        # Convert the Bluesky CID into a valid Qdrant UUID deterministically
        qdrant_uuid = str(uuid.uuid5(NAMESPACE_BLUESKY, cids[i]))
        
        points.append(
            models.PointStruct(
                id=qdrant_uuid, 
                vector=embeddings[i].tolist(),
                payload={
                    "text": texts[i],
                    "did": dids[i],
                    "rkey": rkeys[i],
                    "cid": cids[i],
                    "time_us": times[i]
                }
            )
        )
    
    # Upsert the batch into Qdrant
    client.upsert(
        collection_name=COLLECTION_NAME,
        points=points
    )
    
    total_processed += len(texts)

# Final print to catch the end
elapsed = time.time() - start_time
print(f"Ingestion completely finished! Total: {total_processed:,} records in {elapsed:.2f} seconds.")
