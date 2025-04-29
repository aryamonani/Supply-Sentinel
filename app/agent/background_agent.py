import asyncio
from app.services.weather_service import fetch_weather_data, calculate_weather_risk

from app.services.news_service import fetch_news_headlines, calculate_news_risk_score
from app.services.sentiment_service import analyze_sentiment
from app.services.gemini_service import ask_gemini
from app.services.db_services import fetch_all_centers, update_facility_risk, save_risk_snapshot, fetch_at_risk_facilities
from datetime import datetime


def calculate_overall_risk_score(x_risk: float, weather_risk: float, news_risk: float) -> int:
    overall_score = (x_risk * 0.2 + weather_risk * 0.6 + news_risk * 0.2) * 100
    return round(overall_score)

async def run_agent_once():
    centers = fetch_all_centers() 

    print(f'All facilities fetched: {len(centers)}')


    for facility in centers:
        try:
            lat = facility['Latitude']
            lon = facility['Longitude']
            fc_id = facility['FC_ID']

            print(f"\nFetching risk for {fc_id} ({facility['FC_Name']})...")

            # Fetch and calculate risks
            weather_data = await fetch_weather_data(lat, lon)
            weather_risk = calculate_weather_risk(weather_data) if weather_data else 0.0

            sample_text = "Massive delays labor strike shipment disruptions rallies."
            sentiment_result = await analyze_sentiment(sample_text)
            sentiment_label = list(sentiment_result.keys())[0]
            x_risk = sentiment_result["negative"] if sentiment_label == "negative" else 0.0

            headlines = await fetch_news_headlines("port strike")
            news_risk = await calculate_news_risk_score(headlines) if headlines else 0.0

            # Calculate overall risk
            overall_risk = calculate_overall_risk_score(x_risk, weather_risk, news_risk)

            # Update Risk_Score in facilities collection
            update_facility_risk(fc_id, overall_risk)

            # Save full snapshot for history (optional but recommended)
            save_risk_snapshot({
                "timestamp": datetime.utcnow(),
                "FC_ID": fc_id,
                "weather_risk": weather_risk,
                "news_risk": news_risk,
                "x_risk": x_risk,
                "overall_risk": overall_risk,
                "recommendation": "TBD - Later from Gemini if needed"
            })

            print(f"Updated {fc_id}: Overall Risk {overall_risk}%")


        except Exception as e:
            print(f"Error processing {facility['FC_Name']}: {str(e)}")

    print(f'Checking for at-risk facilities...')

    
    at_risk_fcs = fetch_at_risk_facilities()
    for fc in at_risk_fcs:
                fc_id = fc['FC_ID']
                fc_name = fc['FC_Name']
                fc_risk_score = fc['Risk_Score']

                print(f"Facility {fc_id} - {fc_name} is at risk with {fc_risk_score}% risk")
                
                if fc_risk_score > 30:
                    prompt = f"""
                    You are an AI Supply Chain Risk Advisor.

                    - Fulfillment Center: {fc_name} ({fc_id})
                    - Current Overall Risk Score: {fc_risk_score}%

                    Based on the high risk level, recommend prioritized immediate actions 
                    such as rerouting shipments, stockpiling goods, delaying dispatches, 
                    or any other strategic mitigation actions for this facility.
                    """
                    
                    gemini_response = await ask_gemini(prompt)
                    print(f"Gemini recommendation for {fc_name}: {gemini_response}")

async def start_agent_loop():
    while True:
        await run_agent_once()
        print("\nWaiting for 5 min before next risk fetch...")
        await asyncio.sleep(300) 


if __name__ == "__main__":
    import asyncio
    asyncio.run(start_agent_loop())
