<<<<<<< HEAD
# Service for fetching sentiment data from X and analyzing with TextBlob
=======
from transformers import pipeline
from textblob import TextBlob
import os
from dotenv import load_dotenv

load_dotenv()

# Load HuggingFace model
sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

async def analyze_sentiment(text: str):
    try:
        result = sentiment_pipeline(text)[0] 
        
        if result:
            label = result['label']
            score = result['score']
            return {label.lower(): round(score, 2)}
    
    except Exception as e:
        print(f"HuggingFace error, falling back to TextBlob: {e}")

    # Fallback to TextBlob
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity

    if polarity > 0.2:
        return {"positive": polarity}
    elif polarity < -0.2:
        return {"negative": abs(polarity)}
    else:
        return {"neutral": 1 - abs(polarity)}
>>>>>>> main
