from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import os
import logging
from app.embeddings import VectorStore
from app.llm import generate_answer
from app.schemas import Document, Query, QueryResponse

app = FastAPI(title="RAG System API")

vector_store = VectorStore()

@app.post("/ingest", summary="Ingest documents into the vector store")
async def ingest_documents(documents: List[Document]):
    """Store document embeddings in the vector database."""
    try:
        vector_store.ingest_documents([doc.text for doc in documents])
        return {"message": f"Successfully ingested {len(documents)} documents"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query", response_model=QueryResponse, summary="Query the RAG system")
async def query_rag(query: Query):
    """Retrieve relevant documents and generate an answer."""
    try:
        # Retrieve relevant documents
        relevant_docs = vector_store.search(query.text, k=query.top_k)
        
        # Generate answer using LLM
        answer = generate_answer(query.text, relevant_docs)
        
        return {
            "query": query.text,
            "answer": answer,
            "relevant_documents": relevant_docs
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
