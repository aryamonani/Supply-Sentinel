<<<<<<< HEAD
# Service for fetching news data from NewsAPI
=======
import os
import requests
from dotenv import load_dotenv
from transformers import pipeline
from typing import List

load_dotenv()

NEWS_API_KEY = os.getenv('NEWS_API_KEY')

# Load HuggingFace model for binary sentiment
sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

# Disruption keywords list
disruption_keywords = ["disruption", "tariff", "delay"]

async def fetch_news_headlines(query: str, from_date: str = None) -> List[str]:
    """
    Fetch news headlines based on a query term from NewsAPI.
    """
    try:
        url = f"https://newsapi.org/v2/everything?q={query}&apiKey={NEWS_API_KEY}&pageSize=10&sortBy=publishedAt"
        if from_date:
            url += f"&from={from_date}"

        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            news_data = response.json()
            return [article['title'] for article in news_data.get('articles', [])]
        else:
            print(f"Error fetching news: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"Exception while fetching news: {str(e)}")
        return []

async def calculate_news_risk_score(headlines: List[str]) -> float:
    """
    Analyze news headlines for sentiment and disruption keywords, and calculate risk score.
    """
    if not headlines:
        return 0.0

    disruption_hits = 0

    for headline in headlines:
        # Sentiment Analysis
        result = sentiment_pipeline(headline)[0]  # {"label": "POSITIVE", "score": 0.96}

        label = result['label']

        # If NEGATIVE sentiment
        if label.upper() == "NEGATIVE":
            # Check if headline contains disruption keywords
            if any(keyword in headline.lower() for keyword in disruption_keywords):
                disruption_hits += 1

    risk_score = disruption_hits / len(headlines)
    return round(risk_score, 2)
>>>>>>> main
