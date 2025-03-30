import os
from typing import List
from dotenv import load_dotenv
import openai

# Load environment variables to pass the api key
load_dotenv()
groq_api_key = os.getenv("GROQ_API_KEY")

if not groq_api_key:
    raise ValueError("Groq API key not found. Get one at https://console.groq.com/keys")

# Initialize Groq client (uses OpenAI-compatible endpoint)
openai.api_key = groq_api_key
openai.api_base = "https://api.groq.com/openai/v1"

def generate_answer(query: str, context: List[str]) -> str:
    """Generate an answer using Groq's Llama 3 70B with RAG context."""
    if not context:
        return "No relevant documents found to answer the question."
    
    # Prepare the prompt
    context_str = "\n\n".join([f"Document {i+1}: {doc}" for i, doc in enumerate(context)])
    
    messages = [
        {
            "role": "system",
            "content": "You are a helpful assistant that answers questions based strictly on the provided context."
        },
        {
            "role": "user",
            "content": f"Context:\n{context_str}\n\nQuestion: {query}\n\nAnswer:"
        }
    ]
    
    try:
        response = openai.ChatCompletion.create(
            model="llama3-70b-8192",  # Groq's Llama 3 70B model
            messages=messages,
            temperature=0.7,
            max_tokens=1024
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Error generating answer: {e}"