import os
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

GOOGLE_API_KEY = os.getenv('GEMINI_API_KEY')

genai.configure(api_key=GOOGLE_API_KEY)

def ask_gemini(prompt: str) -> str:
    """
    Send a prompt to Gemini and return the response text.
    """
    model = genai.GenerativeModel(model_name='models/gemini-1.5-pro-latest')
    response = model.generate_content(prompt)
    return response.text
