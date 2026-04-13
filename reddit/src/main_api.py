from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import json
import os

app = FastAPI()

# This allows your React dashboard (usually on localhost:3000) to talk to this API
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/api/sentiment-trends")
def get_trends():
    # Path to the Spark output we created
    file_path = "../reddit/output/time_series_sentiment/part-00000.json"
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return [json.loads(line) for line in f]
    return {"error": "Sentiment data not found. Run Spark script first."}

@app.get("/api/chat")
async def chat_with_expert(query: str):
    from rag_reddit import ask_reddit_expert
    # This calls your ChromaDB + Gemini logic
    answer = ask_reddit_expert(query)
    return {"answer": answer}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)