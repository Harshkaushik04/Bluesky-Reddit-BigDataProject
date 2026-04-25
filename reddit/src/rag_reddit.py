import chromadb
from google import genai
import os

def ask_reddit_expert(question):
    # 1. Setup New Gemini Client (Standard v1 is now the default)
    # Ensure your API key is correct here
    client = genai.Client(api_key="AIzaSyAFRX1qF6zu5OCeebwV76AQ4v2KoX93HKQ")

    # 2. Connect to your existing ChromaDB
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "..", "output", "vector_db")
    
    chroma_client = chromadb.PersistentClient(path=db_path)
    collection = chroma_client.get_collection(name="reddit_knowledge")

    # 3. RETRIEVAL: Search for relevant posts
    print(f"🔍 Searching your database for: '{question}'...")
    results = collection.query(
        query_texts=[question],
        n_results=3
    )

    context = "\n---\n".join(results['documents'][0])

    # 4. PROMPT: Standard RAG Instructions
    prompt = f"""
    You are a Big Data Analyst. Answer based ONLY on the provided REDDIT DATA.
    
    REDDIT DATA:
    {context}
    
    USER QUESTION: 
    {question}
    """

    # 5. GENERATION: Using the new stable 2.0 model
    # 5. GENERATION: Using the specific preview model ID
    try:
        response = client.models.generate_content(
            model="gemini-3-flash-preview", # Add '-preview' here
            contents=prompt
        )
        
        print("\n" + "="*50)
        print("🤖 REDDIT AI EXPERT ANSWER:")
        print("="*50)
        print(response.text)
        print("="*50)
        
    except Exception as e:
        print(f"❌ API Error: {e}")

if __name__ == "__main__":
    user_query = input("\nAsk a question about your Reddit data: ")
    ask_reddit_expert(user_query)