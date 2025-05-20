import requests
from pymongo import MongoClient
import schedule
import time
import praw
import feedparser
from fake_useragent import UserAgent
from datetime import datetime
import pytz
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configuration
# Reddit API Credentials
REDDIT_CLIENT_ID = "hKX4h8j9XUWGTdrD0d_TqQ"
REDDIT_CLIENT_SECRET = "JqUH_KsLg-adUyPCX-Fd4v4Ydbr-Ug"
REDDIT_USER_AGENT = "aryamonani"

# API Endpoints
WEATHER_URL = "https://api.open-meteo.com/v1/forecast?latitude={}&longitude={}&current_weather=true"  

# RSS Feeds for News, Labor, and Logistics
REUTERS_RSS = "http://feeds.reuters.com/reuters/businessNews"
FREIGHTWAVES_RSS = "https://www.freightwaves.com/feed"
SUPPLYCHAIN247_RSS = "https://www.supplychain247.com/feed"

# MongoDB Setup
MONGO_URI = "mongodb+srv://arya-monani:t7gc2tHqtwli41sk@cluster0.cprme1k.mongodb.net/"
client = MongoClient(MONGO_URI)
db = client["supply_chain_db"]
weather_collection = db["weather"]
news_collection = db["news"]
social_media_collection = db["social_media"]
labor_collection = db["labor"]
logistics_collection = db["logistics"]

# Set TTL index for social media data (24 hours = 86,400 seconds)
social_media_collection.drop_indexes()
social_media_collection.create_index("timestamp", expireAfterSeconds=86400)

# User Agent for RSS scraping
ua = UserAgent()

# Unique cities with coordinates (lat, lon) and subreddits
locations = [
    ("New York", 40.7128, -74.0060, "nyc"),
    ("Buffalo", 42.8864, -78.8784, "buffalony"),
    ("Rochester", 43.1566, -77.6088, "rochesterny"),
    ("Newark", 40.7357, -74.1724, "newark"),
    ("Jersey City", 40.7178, -74.0431, "jerseycity"),
    ("Paterson", 40.9165, -74.1718, "paterson"),
    ("Philadelphia", 39.9526, -75.1652, "philadelphia"),
    ("Pittsburgh", 40.4406, -79.9959, "pittsburgh"),
    ("Allentown", 40.6023, -75.4714, "allentown"),
    ("Boston", 42.3601, -71.0589, "boston"),
    ("Worcester", 42.2626, -71.8023, "worcesterma"),
    ("Springfield", 42.1015, -72.5898, "springfieldma"),
    ("Baltimore", 39.2904, -76.6122, "baltimore"),
    ("Silver Spring", 38.9907, -77.0261, "silverspring"),
    ("Frederick", 39.4143, -77.4105, "frederickmd"),
    ("Washington", 38.9072, -77.0369, "washingtondc")
]

# Keywords for Supply Chain and Local News
keywords = ["supply chain", "delay", "port", "logistics"]

# Reddit API Setup
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# Setup requests session with retry logic
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Helper function to get current time in EST
def get_est_datetime():
    est = pytz.timezone("America/New_York")
    return datetime.now(est).strftime("%Y-%m-%d %H:%M:%S %Z")

# Fetch Weather Data (Open-Meteo)
def fetch_weather():
    count = 0
    est_time = get_est_datetime()
    for city, lat, lon, _ in locations:
        try:
            url = WEATHER_URL.format(lat, lon)
            response = session.get(url, timeout=5)
            response.raise_for_status()  # Raise an exception for bad status codes
            if response.status_code == 200:
                data = response.json()
                weather_doc = {
                    "location": city,
                    "lat": lat,
                    "lon": lon,
                    "weather": data.get("current_weather", {}).get("weathercode", None),
                    "temp": data.get("current_weather", {}).get("temperature", None),
                    "timestamp": time.time(),
                    "est_datetime": est_time
                }
                weather_collection.insert_one(weather_doc)
                print(f"Stored weather data for {city} at {est_time}")
                count += 1
            else:
                print(f"Failed to fetch weather for {city}: Status {response.status_code}, Response: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather for {city}: {str(e)}")
    print(f"Total weather documents stored: {count}")

# Fetch News Data (Reuters RSS)
def fetch_news():
    count = 0
    try:
        headers = {'User-Agent': ua.random}
        feed = feedparser.parse(REUTERS_RSS, agent=headers['User-Agent'])
        for entry in feed.entries[:5]:
            city_found = False
            for city, _, _, _ in locations:
                if city.lower() in entry.title.lower() or city.lower() in entry.summary.lower():
                    news_doc = {
                        "location": city,
                        "source": "news",
                        "title": entry.title,
                        "description": entry.summary,
                        "timestamp": time.time()
                    }
                    news_collection.insert_one(news_doc)
                    print(f"Stored news data for {city}")
                    count += 1
                    city_found = True
            if not city_found:
                news_doc = {
                    "location": "General",
                    "source": "news",
                    "title": entry.title,
                    "description": entry.summary,
                    "timestamp": time.time()
                }
                news_collection.insert_one(news_doc)
                print(f"Stored general news data: {entry.title}")
                count += 1
        time.sleep(1)
    except Exception as e:
        print(f"Error fetching news: {str(e)}")
    print(f"Total news documents stored: {count}")

# Fetch Social Media Data (Reddit every 5 minutes)
def fetch_social_media():
    count = 0
    current_time = time.time()
    # General Subreddits
    general_subreddits = ["supplychain", "logistics"]
    for city, _, _, city_subreddit in locations:
        # General Subreddits
        for subreddit in general_subreddits:
            for keyword in keywords:
                try:
                    query = f"{keyword} {city}"
                    submissions = reddit.subreddit(subreddit).search(query, sort="new", limit=5)
                    found = False
                    for submission in submissions:
                        post_age = current_time - submission.created_utc
                        if post_age <= 86400:  # Less than 24 hours old
                            post_doc = {
                                "location": city,
                                "source": "social_media_reddit_general",
                                "subreddit": subreddit,
                                "title": submission.title,
                                "text": submission.selftext,
                                "created_utc": submission.created_utc,
                                "timestamp": current_time
                            }
                            social_media_collection.insert_one(post_doc)
                            print(f"Stored Reddit social media data for {city} in r/{subreddit}: {keyword} (Age: {post_age/3600:.2f} hours)")
                            count += 1
                            found = True
                        else:
                            print(f"Reddit post in r/{subreddit} for {city} is too old: {post_age/3600:.2f} hours")
                    if not found:
                        print(f"No recent Reddit posts found for {city} in r/{subreddit}: {keyword}")
                except Exception as e:
                    print(f"Error fetching Reddit social media for {city} in r/{subreddit}: {str(e)}")

        # City-Specific Subreddit
        if city_subreddit:
            for keyword in keywords:
                try:
                    query = f"{keyword}"
                    submissions = reddit.subreddit(city_subreddit).search(query, sort="new", limit=5)
                    found = False
                    for submission in submissions:
                        post_age = current_time - submission.created_utc
                        if post_age <= 86400:  # Less than 24 hours old
                            post_doc = {
                                "location": city,
                                "source": "social_media_reddit_local",
                                "subreddit": city_subreddit,
                                "title": submission.title,
                                "text": submission.selftext,
                                "created_utc": submission.created_utc,
                                "timestamp": current_time
                            }
                            social_media_collection.insert_one(post_doc)
                            print(f"Stored Reddit local social media data for {city} in r/{city_subreddit}: {keyword} (Age: {post_age/3600:.2f} hours)")
                            count += 1
                            found = True
                        else:
                            print(f"Reddit post in r/{city_subreddit} for {city} is too old: {post_age/3600:.2f} hours")
                    if not found:
                        print(f"No recent Reddit posts found for {city} in r/{city_subreddit}: {keyword}")
                except Exception as e:
                    print(f"Error fetching Reddit local social media for {city} in r/{city_subreddit}: {str(e)}")
    print(f"Total Reddit social media documents stored: {count}")

# Fetch Labor Data (Reuters RSS for labor news)
def fetch_labor_data():
    count = 0
    try:
        headers = {'User-Agent': ua.random}
        feed = feedparser.parse(REUTERS_RSS, agent=headers['User-Agent'])
        for entry in feed.entries[:5]:
            if "strike" in entry.title.lower() or "labor" in entry.title.lower() or "worker" in entry.title.lower() or "union" in entry.title.lower():
                city_found = False
                for city, _, _, _ in locations:
                    if city.lower() in entry.title.lower() or city.lower() in entry.summary.lower():
                        labor_doc = {
                            "location": city,
                            "source": "labor",
                            "title": entry.title,
                            "description": entry.summary,
                            "timestamp": time.time()
                        }
                        labor_collection.insert_one(labor_doc)
                        print(f"Stored labor data for {city}")
                        count += 1
                        city_found = True
                if not city_found:
                    labor_doc = {
                        "location": "General",
                        "source": "labor",
                        "title": entry.title,
                        "description": entry.summary,
                        "timestamp": time.time()
                    }
                    labor_collection.insert_one(labor_doc)
                    print(f"Stored general labor data: {entry.title}")
                    count += 1
        time.sleep(1)
    except Exception as e:
        print(f"Error fetching labor data: {str(e)}")
    print(f"Total labor documents stored: {count}")

# Fetch Logistics Reports (FreightWaves and Supply Chain 24/7 RSS)
def fetch_logistics_reports():
    count = 0
    est_time = get_est_datetime()
    for feed_url in [FREIGHTWAVES_RSS, SUPPLYCHAIN247_RSS]:
        try:
            headers = {'User-Agent': ua.random}
            feed = feedparser.parse(feed_url, agent=headers['User-Agent'])
            for entry in feed.entries[:5]:
                city_found = False
                for city, _, _, _ in locations:
                    if city.lower() in entry.title.lower() or city.lower() in entry.summary.lower():
                        logistics_doc = {
                            "location": city,
                            "source": "logistics",
                            "title": entry.title,
                            "description": entry.summary,
                            "timestamp": time.time(),
                            "est_datetime": est_time
                        }
                        logistics_collection.insert_one(logistics_doc)
                        print(f"Stored logistics data for {city} at {est_time}")
                        count += 1
                        city_found = True
                if not city_found:
                    logistics_doc = {
                        "location": "General",
                        "source": "logistics",
                        "title": entry.title,
                        "description": entry.summary,
                        "timestamp": time.time(),
                        "est_datetime": est_time
                    }
                    logistics_collection.insert_one(logistics_doc)
                    print(f"Stored general logistics data: {entry.title} at {est_time}")
                    count += 1
            time.sleep(1)
        except Exception as e:
            print(f"Error fetching logistics data: {str(e)}")
    print(f"Total logistics documents stored: {count}")

# Main function to fetch all data (every 5 minutes)
def fetch_all_data():
    print("Fetching all data...")
    fetch_weather()
    fetch_news()
    fetch_social_media()
    fetch_labor_data()
    fetch_logistics_reports()

# Schedule the tasks
schedule.every(5).minutes.do(fetch_all_data)

# Run the scheduler
print("Starting data collection...")
while True:
    schedule.run_pending()
    time.sleep(1)