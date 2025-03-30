# RAG System API

## Features

- Document ingestion and embedding using SentenceTransformers
- Vector similarity search with FAISS
- Answer generation using Groq's Llama 3 70B model
- FastAPI endpoints for /ingest and /query
- Dockerized setup


## Dependencies

- requirements.txt


## Setup Instructions

1. Clone this repository:
   ```bash
   git clone https://github.com/JieyaoMin/BI-K_Assignment.git
   cd BI-K_Assignment/rag-system

2. Update .env file with Groq API key (new API Key create in https://console.groq.com/keys with a few clicks):
   GROQ_API_KEY=your_api_key_here

3. Build and run the Docker containers:
    ```bash
    docker-compose up --build

4. The RAG system will run automatically.



## Test Instructions
1. Preprocess the text to a specific format (e.g., ./data/test_data.json)

2. Run the command in a new terminal for ingest:
    ```bash
   curl -X POST "http://localhost:8000/ingest" \
      -H "Content-Type: application/json" \
      -d @data/test_data.json
      
3. Prepare the Query file with a specific format (e.g., ./test/query.json)

4. Run the command in a new terminal for query:
    ```bash
   curl -X POST "http://localhost:8000/query" \
      -H "Content-Type: application/json" \
      -d @test/query.json
