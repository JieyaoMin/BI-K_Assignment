from pydantic import BaseModel
from typing import List, Optional

class Document(BaseModel):
    text: str

class Query(BaseModel):
    text: str
    top_k: int = 3

class QueryResponse(BaseModel):
    query: str
    answer: str
    relevant_documents: List[str]
    