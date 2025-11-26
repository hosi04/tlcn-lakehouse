import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI

load_dotenv()

MODEL_ID = "gemini-2.0-flash"

def get_llm():
    """
    Khởi tạo LLM dùng Google AI Studio (miễn phí)
    """
    llm = ChatGoogleGenerativeAI(
        model=MODEL_ID,
        api_key=os.getenv("GEMINI_API_KEY"),
        temperature=0.0,
        max_output_tokens=None
    )
    return llm