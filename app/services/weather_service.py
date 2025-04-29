<<<<<<< HEAD
# Service for fetching real-world weather data from OpenWeatherMap API
=======
import os
import requests
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Fetch your API key securely
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')

async def fetch_weather_data(lat: float, lon: float) -> dict:

    try:
        url = (
            f"https://api.openweathermap.org/data/2.5/weather?"
            f"lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
        )
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            weather_data = response.json()
            return weather_data
        else:
            print(f"Error fetching weather data: {response.status_code} - {response.text}")
            return {}
        
    except Exception as e:
        print(f"Exception while fetching weather data: {str(e)}")
        return {}

def calculate_weather_risk(weather_data: dict) -> float:
    """
    Calculate composite weather risk score based on multiple factors:
    - Rainfall (rain.1h)
    - Wind speed
    - Visibility
    - Main weather type
    """
    score = 0.0

    # 1. Rain condition
    rain = weather_data.get('rain', {}).get('1h', 0.0)
    if rain > 0.5:
        score += 0.4

    # 2. Wind speed
    wind_speed = weather_data.get('wind', {}).get('speed', 0.0)
    if wind_speed > 10:
        score += 0.3

    # 3. Visibility
    visibility = weather_data.get('visibility', 10000)  # Default good visibility if missing
    if visibility < 1000:
        score += 0.2

    # 4. Main Weather Condition
    weather_main = None
    if 'weather' in weather_data and len(weather_data['weather']) > 0:
        weather_main = weather_data['weather'][0]['main']
    
    if weather_main in ["Storm", "Thunderstorm"]:
        score += 0.1

    # Cap the score to 1.0
    final_score = min(score, 1.0)

    return round(final_score, 2)
>>>>>>> main
