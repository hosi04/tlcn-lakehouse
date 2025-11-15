import os
from langchain_google_vertexai import ChatVertexAI

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./chatbot-auth.json"

PROJECT_ID = "chatbot-myauris"
LOCATION = "us-central1"
MODEL_ID = "gemini-2.5-flash"

def get_vertex_llm():
    """
    Khởi tạo LLM từ Vertex AI dùng JSON credential
    """
    llm = ChatVertexAI(
        model=MODEL_ID,
        project=PROJECT_ID,
        location=LOCATION,
        temperature=0.0,
        max_output_tokens=800
    )
    return llm
