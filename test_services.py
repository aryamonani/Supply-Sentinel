import asyncio

from app.services.news_service import fetch_news_headlines, calculate_news_risk_score
from app.services.weather_service import fetch_weather_data, calculate_weather_risk
from app.services.sentiment_service import analyze_sentiment
from app.services.gemini_service import ask_gemini


def calculate_overall_risk_score(x_risk: float, weather_risk: float, news_risk: float) -> int:
    """
    Calculate Overall Risk Score using weighted average:
    - X Activity: 20%
    - Weather: 60%
    - News: 20%
    Returns the score as an integer (0-100).
    """
    overall_score = (x_risk * 0.2 + weather_risk * 0.6 + news_risk * 0.2) * 100
    return round(overall_score)

async def main():
    lat, lon = 40.7128, -74.0060

    weather_risk = 0.0
    news_risk = 0.0
    x_risk = 0.0

    # Test Weather Service
    print("\nTesting Weather Service...")
    weather_data = await fetch_weather_data(lat, lon)

    if weather_data:
        weather_risk = calculate_weather_risk(weather_data)
        print("Calculated Weather Risk Score:", weather_risk)
    else:
        print("No weather data fetched.")

    # Test News Service
    print("\nTesting News Service...")
    query = "port strike"
    headlines = await fetch_news_headlines(query)

    if headlines:
        news_risk = await calculate_news_risk_score(headlines)
        print("News Risk Score:", news_risk)
    else:
        print("No headlines fetched.")

    # Test Sentiment Service
    print("\nTesting Sentiment Service...")
    sample_text = "Massive delays supply chain labor strikes."

    try:
        sentiment_result = await analyze_sentiment(sample_text)
        sentiment_label = list(sentiment_result.keys())[0]

        if sentiment_label == "negative":
            x_risk = sentiment_result["negative"]
        else:
            x_risk = 0.0

        print(f"Sentiment Result: {sentiment_result}")
        print("X/Twitter Risk Score (based on sentiment):", x_risk)

    except Exception as e:
        print(f"Error analyzing sentiment: {str(e)}")

    # Calculate and display overall risk
    print("\nCalculating Final Overall Risk Score...")
    overall_risk = calculate_overall_risk_score(x_risk, weather_risk, news_risk)
    print(f"Overall Risk Score: {overall_risk}%")

    if overall_risk > 70:
        print("Fulfillment Center Status: AT RISK")
    else:
        print("Fulfillment Center Status: SAFE")

    #test Gemini Service
    prompt = f"""
You are an AI Supply Chain Risk Advisor for a logistics company.

You have access to the following live risk scores:

- Weather Risk Score: {weather_risk}
- News Risk Score: {news_risk}
- X/Twitter Sentiment Risk Score: {x_risk}
- Overall Risk Score: {overall_risk}%

Context:
- A Weather Risk > 0.6 usually indicates storms, floods, or extreme events.
- A News Risk > 0.5 suggests active political events, strikes, disruptions.
- A Sentiment Risk > 0.5 suggests negative public behavior or unrest.
- If Overall Risk Score > 70%, immediate action is critical.

Task:
Based on the scores above, recommend 2-3 prioritized actions to minimize supply chain disruption.
The advice should be realistic, practical, and directly executable (e.g., reroute, delay, stockpile, communicate).

Format the response as:
1. Action 1
2. Action 2
3. Action 3 (if necessary)

Avoid generic suggestions. Be sharp, direct, and professional.
"""
    # Ask Gemini
    advice = ask_gemini(prompt)
    print("\nGemini's Recommendation:")
    print(advice)

if __name__ == "__main__":
    asyncio.run(main())
