#!/usr/bin/env python3

import requests
from pymongo import MongoClient
import schedule
import time
import praw
import feedparser
from fake_useragent import UserAgent
from datetime import datetime, timedelta
import pytz
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT")
MONGO_URI = os.environ.get("MONGO_URI")
NEWS_API_KEY = os.environ.get("NEWS_API_KEY")

# Check if all required environment variables are set
required_env_vars = [
    "REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT", "MONGO_URI", "NEWS_API_KEY"
]
for var in required_env_vars:
    if not os.environ.get(var):
        logger.error(f"Missing environment variable in data_pull.py: {var}. Please set it in the .env file.")
        raise SystemExit(f"Exiting due to missing environment variable: {var}")

# API Endpoints
WEATHER_URL = "https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true&hourly=temperature_2m,wind_speed_120m,wind_speed_180m,rain,showers,snowfall,snow_depth,visibility,temperature_80m,temperature_120m,temperature_180m,wind_speed_80m,wind_speed_10m,apparent_temperature&forecast_days=16"
NEWS_API_URL = "https://newsapi.org/v2/everything"
FREIGHTWAVES_RSS = "https://www.freightwaves.com/feed"
SUPPLYCHAIN247_RSS = "https://www.supplychain247.com/feed"

# MongoDB Setup
try:
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=60000,
        connectTimeoutMS=60000,
        retryWrites=True,
        retryReads=True,
        maxPoolSize=10
    )
    client.server_info()
    logger.info("Successfully connected to MongoDB Atlas")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB Atlas: {str(e)}")
    raise SystemExit(f"Exiting due to database connection failure: {str(e)}")

db = client["supply_chain_db"]
weather_collection = db["weather"]
news_collection = db["news"]
social_media_collection = db["social_media"]
labor_collection = db["labor"]
logistics_collection = db["logistics"]

# Set TTL indexes for all collections (24 hours = 86,400 seconds) on timestamp field
collections = [weather_collection, news_collection, social_media_collection, labor_collection, logistics_collection]
for collection in collections:
    try:
        collection.drop_indexes()
        collection.create_index("timestamp", expireAfterSeconds=86400)
        logger.info(f"TTL index ensured for {collection.name} on timestamp field.")
    except Exception as e:
        logger.warning(f"Could not drop/create TTL index on {collection.name}: {e}")

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
keywords = [
    "supply chain", "delay", "port", "logistics", "traffic", "protest", "strike",
    "road closure", "weather alert", "event", "disruption", "outage", "shutdown",
    "delivery", "shipping", "transport", "warehouse", "distribution"
]

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

# Helper function to get current time in EST as datetime object
def get_est_datetime():
    est = pytz.timezone("America/New_York")
    return datetime.now(est)

# Fetch Weather Data (Open-Meteo)
def fetch_weather():
    count = 0
    est_time = get_est_datetime()
    current_timestamp = time.time()
    for city, lat, lon, _ in locations:
        try:
            url = WEATHER_URL.format(latitude=lat, longitude=lon)
            response = session.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            weather_doc = {
                "location": city,
                "lat": lat,
                "lon": lon,
                "weather": data.get("current_weather", {}).get("weathercode", None),
                "temp": data.get("current_weather", {}).get("temperature", None),
                "windspeed": data.get("current_weather", {}).get("windspeed", None),
                "est_datetime": est_time,
                "timestamp": current_timestamp
            }
            existing_docs = weather_collection.count_documents({"location": city})
            logger.info(f"Documents for {city} before update: {existing_docs}")
            result = weather_collection.update_one(
                {"location": city, "est_datetime": est_time},
                {"$set": weather_doc},
                upsert=True
            )
            logger.info(f"Update result for {city}: matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")
            inserted_doc = weather_collection.find_one({"location": city, "est_datetime": est_time})
            if inserted_doc:
                logger.info(f"Verified: Document found for {city} at {est_time}")
            else:
                logger.error(f"Failed to find document for {city} at {est_time}")
            logger.info(f"Documents for {city} after update: {weather_collection.count_documents({'location': city})}")
            count += 1
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching weather for {city}: {str(e)}")
    logger.info(f"Total weather documents stored: {count}")

# Fetch News Data (NewsAPI)
def fetch_news():
    count = 0
    est_time = get_est_datetime()
    current_timestamp = time.time()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    for city, _, _, _ in locations:
        try:
            query = f"{city} AND (supply chain OR weather OR disruption OR logistics)"
            params = {
                "q": query,
                "language": "en",
                "from": yesterday,
                "sortBy": "publishedAt",
                "apiKey": NEWS_API_KEY
            }
            response = session.get(NEWS_API_URL, params=params)
            response.raise_for_status()
            articles = response.json().get("articles", [])

            for article in articles[:10]:
                news_doc = {
                    "location": city,
                    "source": "news",
                    "title": article.get("title", "N/A"),
                    "description": article.get("description", "N/A"),
                    "est_datetime": est_time,
                    "url": article.get("url", None),
                    "timestamp": current_timestamp
                }
                existing_docs = news_collection.count_documents({"location": city})
                logger.info(f"Documents for {city} before update: {existing_docs}")
                result = news_collection.update_one(
                    {"title": article.get("title"), "location": city, "est_datetime": est_time},
                    {"$set": news_doc},
                    upsert=True
                )
                logger.info(f"Update result for {city}: matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")
                inserted_doc = news_collection.find_one({"title": article.get("title"), "location": city, "est_datetime": est_time})
                if inserted_doc:
                    logger.info(f"Verified: News document found for {city}: {article.get('title')[:50]}...")
                else:
                    logger.error(f"Failed to find news document for {city}: {article.get('title')[:50]}...")
                logger.info(f"Documents for {city} after update: {news_collection.count_documents({'location': city})}")
                count += 1
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching news for {city}: {str(e)}")
    logger.info(f"Total news documents stored: {count}")

# Fetch Social Media Data (Reddit)
def fetch_social_media():
    count = 0
    est_time = get_est_datetime()
    current_timestamp = time.time()
    general_subreddits = ["supplychain", "logistics", "news"]
    
    try:
        reddit_instance = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
    except Exception as e:
        logger.error(f"Failed to initialize PRAW/Reddit instance: {e}")
        return

    for city_name, _, _, city_subreddit_name in locations:
        subreddits_to_check = general_subreddits.copy()
        if city_subreddit_name:
            subreddits_to_check.append(city_subreddit_name)

        for subreddit_name in set(subreddits_to_check):
            for keyword in keywords:
                try:
                    query_text = f"{keyword} {city_name}" if subreddit_name in general_subreddits else f"{keyword}"
                    submissions = reddit_instance.subreddit(subreddit_name).search(query_text, sort="new", limit=15, time_filter="day")
                    
                    for submission in submissions:
                        if (time.time() - submission.created_utc) <= 86400:
                            post_doc = {
                                "reddit_id": submission.id,
                                "location": city_name,
                                "source": "social_media_reddit",
                                "subreddit": submission.subreddit.display_name,
                                "title": submission.title,
                                "text": submission.selftext if submission.selftext else submission.title,
                                "created_utc": submission.created_utc,
                                "est_datetime": est_time,
                                "url": submission.url,
                                "permalink": f"https://www.reddit.com{submission.permalink}",
                                "score": submission.score,
                                "num_comments": submission.num_comments,
                                "timestamp": current_timestamp
                            }
                            existing_docs = social_media_collection.count_documents({"location": city_name})
                            logger.info(f"Documents for {city_name} before update: {existing_docs}")
                            result = social_media_collection.update_one(
                                {"reddit_id": submission.id, "location": city_name},
                                {"$set": post_doc},
                                upsert=True
                            )
                            logger.info(f"Update result for {city_name}: matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")
                            inserted_doc = social_media_collection.find_one({"reddit_id": submission.id, "location": city_name})
                            if inserted_doc:
                                logger.info(f"Verified: Reddit post found: {submission.id} for {city_name} in r/{subreddit_name}")
                            else:
                                logger.error(f"Failed to find Reddit post: {submission.id} for {city_name} in r/{subreddit_name}")
                            logger.info(f"Documents for {city_name} after update: {social_media_collection.count_documents({'location': city_name})}")
                            count += 1
                except praw.exceptions.PRAWException as e:
                    logger.error(f"PRAW Error fetching Reddit social media for {city_name} in r/{subreddit_name} (Query: '{query_text}'): {e}")
                except Exception as e:
                    logger.error(f"General Error fetching Reddit social media for {city_name} in r/{subreddit_name} (Query: '{query_text}'): {e}")
    logger.info(f"Total Reddit social media documents stored: {count}")

# Fetch Labor Data (Reuters RSS for labor news)
def fetch_labor_data():
    count = 0
    est_time = get_est_datetime()
    current_timestamp = time.time()
    labor_keywords = ["strike", "labor", "worker", "union", "contract", "wage", "hiring", "unemployment"]
    try:
        headers = {'User-Agent': ua.random}
        feed = feedparser.parse("http://feeds.reuters.com/reuters/businessNews", agent=headers['User-Agent'])
        for entry in feed.entries[:10]:
            title = getattr(entry, 'title', "N/A")
            summary = getattr(entry, 'summary', "N/A")
            link = getattr(entry, 'link', None)

            if any(lk in title.lower() or lk in summary.lower() for lk in labor_keywords):
                city_found = False
                for city, _, _, _ in locations:
                    if city.lower() in title.lower() or city.lower() in summary.lower():
                        labor_doc = {
                            "location": city,
                            "source": "labor",
                            "title": title,
                            "description": summary,
                            "est_datetime": est_time,
                            "url": link,
                            "timestamp": current_timestamp
                        }
                        existing_docs = labor_collection.count_documents({"location": city})
                        logger.info(f"Documents for {city} before update: {existing_docs}")
                        result = labor_collection.update_one(
                            {"title": title, "location": city, "est_datetime": est_time},
                            {"$set": labor_doc},
                            upsert=True
                        )
                        logger.info(f"Update result for {city}: matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")
                        inserted_doc = labor_collection.find_one({"title": title, "location": city, "est_datetime": est_time})
                        if inserted_doc:
                            logger.info(f"Verified: Labor document found for {city}: {title[:50]}...")
                        else:
                            logger.error(f"Failed to find labor document for {city}: {title[:50]}...")
                        logger.info(f"Documents for {city} after update: {labor_collection.count_documents({'location': city})}")
                        count += 1
                        city_found = True
                        break
                if not city_found:
                    labor_doc = {
                        "location": "General",
                        "source": "labor",
                        "title": title,
                        "description": summary,
                        "est_datetime": est_time,
                        "url": link,
                        "timestamp": current_timestamp
                    }
                    existing_docs = labor_collection.count_documents({"location": "General"})
                    logger.info(f"Documents for General before update: {existing_docs}")
                    result = labor_collection.update_one(
                        {"title": title, "location": "General", "est_datetime": est_time},
                        {"$set": labor_doc},
                        upsert=True
                    )
                    logger.info(f"Update result for General: matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")
                    inserted_doc = labor_collection.find_one({"title": title, "location": "General", "est_datetime": est_time})
                    if inserted_doc:
                        logger.info(f"Verified: General labor document found: {title[:50]}...")
                    else:
                        logger.error(f"Failed to find general labor document: {title[:50]}...")
                    logger.info(f"Documents for General after update: {labor_collection.count_documents({'location': 'General'})}")
                    count += 1
        time.sleep(1)
    except Exception as e:
        logger.error(f"Error fetching labor data: {str(e)}")
    logger.info(f"Total labor documents stored: {count}")

# Fetch Logistics Reports (FreightWaves and Supply Chain 24/7 RSS)
def fetch_logistics_reports():
    count = 0
    est_time = get_est_datetime()
    current_timestamp = time.time()
    for feed_url in [FREIGHTWAVES_RSS, SUPPLYCHAIN247_RSS]:
        try:
            headers = {'User-Agent': ua.random}
            feed = feedparser.parse(feed_url, agent=headers['User-Agent'])
            for entry in feed.entries[:10]:
                title = getattr(entry, 'title', "N/A")
                summary = getattr(entry, 'summary', "N/A")
                link = getattr(entry, 'link', None)

                city_found = False
                for city, _, _, _ in locations:
                    if city.lower() in title.lower() or city.lower() in summary.lower():
                        logistics_doc = {
                            "location": city,
                            "source": "logistics",
                            "title": title,
                            "description": summary,
                            "est_datetime": est_time,
                            "url": link,
                            "timestamp": current_timestamp
                        }
                        existing_docs = logistics_collection.count_documents({"location": city})
                        logger.info(f"Documents for {city} before update: {existing_docs}")
                        result = logistics_collection.update_one(
                            {"title": title, "location": city, "est_datetime": est_time},
                            {"$set": logistics_doc},
                            upsert=True
                        )
                        logger.info(f"Update result for {city}: matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")
                        inserted_doc = logistics_collection.find_one({"title": title, "location": city, "est_datetime": est_time})
                        if inserted_doc:
                            logger.info(f"Verified: Logistics document found for {city}: {title[:50]}...")
                        else:
                            logger.error(f"Failed to find logistics document for {city}: {title[:50]}...")
                        logger.info(f"Documents for {city} after update: {logistics_collection.count_documents({'location': city})}")
                        count += 1
                        city_found = True
                        break
                if not city_found:
                    logistics_doc = {
                        "location": "General",
                        "source": "logistics",
                        "title": title,
                        "description": summary,
                        "est_datetime": est_time,
                        "url": link,
                        "timestamp": current_timestamp
                    }
                    existing_docs = logistics_collection.count_documents({"location": "General"})
                    logger.info(f"Documents for General before update: {existing_docs}")
                    result = logistics_collection.update_one(
                        {"title": title, "location": "General", "est_datetime": est_time},
                        {"$set": logistics_doc},
                        upsert=True
                    )
                    logger.info(f"Update result for General: matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")
                    inserted_doc = logistics_collection.find_one({"title": title, "location": "General", "est_datetime": est_time})
                    if inserted_doc:
                        logger.info(f"Verified: General logistics document found: {title[:50]}...")
                    else:
                        logger.error(f"Failed to find general logistics document: {title[:50]}...")
                    logger.info(f"Documents for General after update: {logistics_collection.count_documents({'location': 'General'})}")
                    count += 1
            time.sleep(1)
        except Exception as e:
            logger.error(f"Error fetching logistics data: {str(e)}")
    logger.info(f"Total logistics documents stored: {count}")

# Main function to fetch all data
def fetch_all_data():
    logger.info("Starting data fetching...")
    fetch_weather()
    fetch_news()
    fetch_social_media()
    fetch_labor_data()
    fetch_logistics_reports()
    logger.info("Data fetching completed.")

# Schedule the tasks
schedule.every(5).minutes.do(fetch_all_data)

# Run the scheduler
logger.info("Starting data collection...")
while True:
    schedule.run_pending()
    time.sleep(1)