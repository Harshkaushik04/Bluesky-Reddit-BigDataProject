import pyarrow.dataset as ds
import time
from qdrant_client import QdrantClient
from qdrant_client.http import models

# --- Configuration ---
PARQUET_DIR = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/silver/readyForSentimentAnalysis/"
COLLECTION_NAME = "all_posts_and_comments"
BATCH_SIZE = 1000  

print("Connecting to Qdrant...")
client = QdrantClient(url="http://localhost:6333", timeout=100.0)

print("Starting filter-based payload update...")
dataset = ds.dataset(PARQUET_DIR, format="parquet")
total_processed = 0
start_time = time.time()

for batch in dataset.to_batches(batch_size=BATCH_SIZE):
    batch_dict = batch.to_pydict()
    operations = []
    
    commits = batch_dict.get('commit', [])
    raw_dids = batch_dict.get('did', [])
    
    for i in range(len(commits)):
        commit = commits[i]
        
        # Ensure we have a valid struct and the DID exists
        if not commit or not isinstance(commit, dict) or i >= len(raw_dids):
            continue
            
        did = str(raw_dids[i])
        rkey = commit.get('rkey')
        record = commit.get('record')
        
        # We MUST have did and rkey to do the filter lookup
        if not rkey or not record or not isinstance(record, dict):
            continue
            
        # 1. Safely extract the new fields
        reply = record.get('reply')
        facets = record.get('facets')
        langs = record.get('langs')
        
        # 2. Build the payload dynamically (Only add if NOT null)
        new_payload = {}
        if reply is not None:
            new_payload["reply"] = reply
        if facets is not None:
            new_payload["facets"] = facets
        if langs is not None:
            new_payload["langs"] = langs
            
        # If all three were null, skip sending a useless update to Qdrant
        if not new_payload:
            continue
            
        # 3. Create the Filter (Find the vector where did == X AND rkey == Y)
        point_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="did", 
                    match=models.MatchValue(value=did)
                ),
                models.FieldCondition(
                    key="rkey", 
                    match=models.MatchValue(value=rkey)
                )
            ]
        )
        
        # 4. Queue the operation
        operations.append(
            models.SetPayloadOperation(
                set_payload=models.SetPayload(
                    payload=new_payload,
                    filter=point_filter
                )
            )
        )

    # If the entire batch had no valid updates, skip to the next Parquet chunk
    if not operations:
        continue
    
    # 5. Send the batch of filtered updates to Qdrant asynchronously
    client.batch_update_points(
        collection_name=COLLECTION_NAME,
        wait=False, 
        update_operations=operations
    )
    
    total_processed += len(operations)
    
    if total_processed % 50_000 == 0:
        elapsed = time.time() - start_time
        print(f"Sent payload updates for {total_processed:,} records... ({total_processed / elapsed:.2f} updates/sec)")

elapsed = time.time() - start_time
print(f"Payload update completely finished! Total: {total_processed:,} records updated in {elapsed:.2f} seconds.")
