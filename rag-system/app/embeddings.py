import os
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
from typing import List

class VectorStore:
    def __init__(self):
        # Initialize embedding model
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.dimension = 384  # Dimension of all-MiniLM-L6-v2 embeddings
        self.index = None
        self.documents = []
        
    def ingest_documents(self, documents: List[str]):
        """Generate embeddings for documents and store them in FAISS index."""
        self.documents.extend(documents)
        
        # Generate embeddings
        embeddings = self.embedding_model.encode(documents, show_progress_bar=True)
        
        # Initialize FAISS index if it doesn't exist
        if self.index is None:
            self.index = faiss.IndexFlatL2(self.dimension)
            
        # Add embeddings to index
        self.index.add(embeddings)
        
    def search(self, query: str, k: int = 3) -> List[str]:
        """Search for similar documents using FAISS."""
        if self.index is None or len(self.documents) == 0:
            return []
            
        # Generate query embedding
        query_embedding = self.embedding_model.encode([query])
        
        # Search in FAISS index
        distances, indices = self.index.search(query_embedding, k)
        
        # Get relevant documents
        results = [self.documents[i] for i in indices[0]]
        return results
    