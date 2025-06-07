#!/usr/bin/env python3

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
import logging
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
# MongoDB Atlas API Keys (needed for dynamic IP whitelisting - REMOVED below as redundant if 0.0.0.0/0 is active)
# ATLAS_PUBLIC_KEY = os.environ.get("ATLAS_PUBLIC_KEY")
# ATLAS_PRIVATE_KEY = os.environ.get("ATLAS_PRIVATE_KEY")
# PROJECT_ID = os.environ.get("PROJECT_ID")

# Reddit API Credentials
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT")

# MongoDB Connection String
MONGO_URI = os.environ.get("MONGO_URI")

# Check if all required environment variables are set
required_env_vars = [
    "REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT", "MONGO_URI"
]
for var in required_env_vars:
    if not os.environ.get(var):
        logger.error(f"Missing environment variable in data_pull.py: {var}. Please set it in the .env file.")
        raise SystemExit(f"Exiting due to missing environment variable: {var}")

# API Endpoints
WEATHER_URL = "https://api.open-meteo.com/v1/forecast?latitude={}&longitude={}&current_weather=true"
REUTERS_RSS = "http://feeds.reuters.com/reuters/businessNews"
FREIGHTWAVES_RSS = "https://www.freightwaves.com/feed"
SUPPLYCHAIN247_RSS = "https://www.supplychain247.com/feed"

# --- REMOVED DYNAMIC IP WHITELISTING LOGIC ---
# The previous get_public_ip and whitelist_ip functions and their calls are removed.
# This functionality is typically not needed if 0.0.0.0/0 is allowed in MongoDB Atlas Network Access,
# and it often caused 401 errors due to insufficient Atlas API Key permissions.
# Connection issues on specific networks are better handled via VPN or ensuring correct DNS, not dynamic whitelisting from the app.
# --- END REMOVED DYNAMIC IP WHITELISTING LOGIC ---


# MongoDB Setup
try:
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        retryWrites=True,
        retryReads=True
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

# Set TTL index for social media data (24 hours = 86,400 seconds)
try:
    # Drop existing indexes first to ensure unique/correct TTL settings if they change
    social_media_collection.drop_indexes()
    social_media_collection.create_index("timestamp", expireAfterSeconds=86400)
    logger.info("TTL index ensured for social_media_collection.")
except Exception as e:
    logger.warning(f"Could not drop/create TTL index on social_media_collection: {e}")


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

# Keywords for Supply Chain and Local News (Expanded for more potential matches)
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

# Helper function to get current time in EST as string
def get_est_datetime_str():
    est = pytz.timezone("America/New_York")
    return datetime.now(est).strftime("%Y-%m-%d %H:%M:%S %Z")

# Fetch Weather Data (Open-Meteo)
def fetch_weather():
    count = 0
    est_time = get_est_datetime_str()
    for city, lat, lon, _ in locations:
        try:
            url = WEATHER_URL.format(lat, lon)
            response = session.get(url, timeout=10) # Increased timeout for network reliability
            response.raise_for_status()
            data = response.json()
            weather_doc = {
                "location": city,
                "lat": lat,
                "lon": lon,
                "weather": data.get("current_weather", {}).get("weathercode", None),
                "temp": data.get("current_weather", {}).get("temperature", None),
                "windspeed": data.get("current_weather", {}).get("windspeed", None), # Added windspeed
                "description": data.get("current_weather", {}).get("weatherdescription", None), # Added more descriptive weather
                "timestamp": time.time(),
                "est_datetime": est_time
            }
            # Use upsert to prevent strict duplicate entries if running frequently
            weather_collection.update_one(
                {"location": city, "est_datetime": est_time}, # Filter to find unique document for update
                {"$set": weather_doc},
                upsert=True
            )
            logger.info(f"Stored weather data for {city} at {est_time}")
            count += 1
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching weather for {city}: {str(e)}")
    logger.info(f"Total weather documents stored: {count}")

# Fetch News Data (Reuters RSS)
def fetch_news():
    count = 0
    est_time = get_est_datetime_str()
    try:
        headers = {'User-Agent': ua.random}
        feed = feedparser.parse(REUTERS_RSS, agent=headers['User-Agent'])
        for entry in feed.entries[:10]: # Fetch more entries to increase chance of matching
            title = getattr(entry, 'title', "N/A")
            summary = getattr(entry, 'summary', "N/A")
            link = getattr(entry, 'link', None)

            city_found = False
            for city, _, _, _ in locations:
                if city.lower() in title.lower() or city.lower() in summary.lower():
                    news_doc = {
                        "location": city,
                        "source": "news",
                        "title": title,
                        "description": summary,
                        "timestamp": time.time(),
                        "est_datetime": est_time,
                        "url": link # Store URL for future verification
                    }
                    news_collection.update_one(
                        {"title": title, "location": city, "est_datetime": est_time},
                        {"$set": news_doc},
                        upsert=True
                    )
                    logger.info(f"Stored news data for {city}: {title[:50]}...")
                    count += 1
                    city_found = True
                    break # Only store once per city match
            if not city_found:
                news_doc = {
                    "location": "General",
                    "source": "news",
                    "title": title,
                    "description": summary,
                    "timestamp": time.time(),
                    "est_datetime": est_time,
                    "url": link
                }
                news_collection.update_one(
                    {"title": title, "location": "General", "est_datetime": est_time},
                    {"$set": news_doc},
                    upsert=True
                )
                logger.info(f"Stored general news data: {title[:50]}...")
                count += 1
        time.sleep(1) # Be polite to RSS feeds
    except Exception as e:
        logger.error(f"Error fetching news: {str(e)}")
    logger.info(f"Total news documents stored: {count}")

# Fetch Social Media Data (Reddit)
def fetch_social_media():
    count = 0
    current_time = time.time()
    general_subreddits = ["supplychain", "logistics", "news"] # Added 'news' for broader context
    
    # Authenticate and get a fresh Reddit instance
    try:
        reddit_instance = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
    except Exception as e:
        logger.error(f"Failed to initialize PRAW/Reddit instance: {e}")
        return # Exit if Reddit instance fails to initialize

    for city_name, _, _, city_subreddit_name in locations:
        subreddits_to_check = general_subreddits.copy()
        if city_subreddit_name: # Add specific city subreddit if available
            subreddits_to_check.append(city_subreddit_name)

        for subreddit_name in set(subreddits_to_check): # Use set to avoid duplicates
            for keyword in keywords:
                try:
                    # Adjust query for general vs local subreddits
                    query_text = f"{keyword} {city_name}" if subreddit_name in general_subreddits else f"{keyword}" 
                    
                    # Search within the last 24 hours explicitly and increase limit
                    submissions = reddit_instance.subreddit(subreddit_name).search(query_text, sort="new", limit=15, time_filter="day") 
                    
                    for submission in submissions:
                        # Ensure we only store unique posts based on Reddit's ID and within the last 24 hours
                        if (current_time - submission.created_utc) <= 86400:
                            post_doc = {
                                "reddit_id": submission.id, # Store Reddit's unique ID for upsert
                                "location": city_name,
                                "source": "social_media_reddit",
                                "subreddit": submission.subreddit.display_name,
                                "title": submission.title,
                                "text": submission.selftext if submission.selftext else submission.title, # Use title if selftext is empty
                                "created_utc": submission.created_utc,
                                "timestamp": current_time,
                                "url": submission.url,          # <--- STORE DIRECT URL
                                "permalink": f"https://www.reddit.com{submission.permalink}", # <--- STORE PERMALINK
                                "score": submission.score, # Added for potential sentiment/impact
                                "num_comments": submission.num_comments
                            }
                            social_media_collection.update_one(
                                {"reddit_id": submission.id, "location": city_name}, # Use Reddit ID for unique update
                                {"$set": post_doc},
                                upsert=True
                            )
                            logger.info(f"Stored Reddit post: {submission.id} for {city_name} in r/{subreddit_name} (Keyword: {keyword})")
                            count += 1
                except praw.exceptions.PRAWException as e:
                    logger.error(f"PRAW Error fetching Reddit social media for {city_name} in r/{subreddit_name} (Query: '{query_text}'): {e}")
                except Exception as e:
                    logger.error(f"General Error fetching Reddit social media for {city_name} in r/{subreddit_name} (Query: '{query_text}'): {e}")
    logger.info(f"Total Reddit social media documents stored: {count}")


# Fetch Labor Data (Reuters RSS for labor news)
def fetch_labor_data():
    count = 0
    est_time = get_est_datetime_str()
    labor_keywords = ["strike", "labor", "worker", "union", "contract", "wage", "hiring", "unemployment"]
    try:
        headers = {'User-Agent': ua.random}
        feed = feedparser.parse(REUTERS_RSS, agent=headers['User-Agent'])
        for entry in feed.entries[:10]: # Fetch more entries
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
                            "timestamp": time.time(),
                            "est_datetime": est_time,
                            "url": link
                        }
                        labor_collection.update_one(
                            {"title": title, "location": city, "est_datetime": est_time},
                            {"$set": labor_doc},
                            upsert=True
                        )
                        logger.info(f"Stored labor data for {city}: {title[:50]}...")
                        count += 1
                        city_found = True
                        break
                if not city_found:
                    labor_doc = {
                        "location": "General",
                        "source": "labor",
                        "title": title,
                        "description": summary,
                        "timestamp": time.time(),
                        "est_datetime": est_time,
                        "url": link
                    }
                    labor_collection.update_one(
                        {"title": title, "location": "General", "est_datetime": est_time},
                        {"$set": labor_doc},
                        upsert=True
                    )
                    logger.info(f"Stored general labor data: {title[:50]}...")
                    count += 1
        time.sleep(1)
    except Exception as e:
        logger.error(f"Error fetching labor data: {str(e)}")
    logger.info(f"Total labor documents stored: {count}")

# Fetch Logistics Reports (FreightWaves and Supply Chain 24/7 RSS)
def fetch_logistics_reports():
    count = 0
    est_time = get_est_datetime_str()
    for feed_url in [FREIGHTWAVES_RSS, SUPPLYCHAIN247_RSS]:
        try:
            headers = {'User-Agent': ua.random}
            feed = feedparser.parse(feed_url, agent=headers['User-Agent'])
            for entry in feed.entries[:10]: # Fetch more entries
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
                            "timestamp": time.time(),
                            "est_datetime": est_time,
                            "url": link
                        }
                        logistics_collection.update_one(
                            {"title": title, "location": city, "est_datetime": est_time},
                            {"$set": logistics_doc},
                            upsert=True
                        )
                        logger.info(f"Stored logistics data for {city}: {title[:50]}...")
                        count += 1
                        city_found = True
                        break
                if not city_found:
                    logistics_doc = {
                        "location": "General",
                        "source": "logistics",
                        "title": title,
                        "description": summary,
                        "timestamp": time.time(),
                        "est_datetime": est_time,
                        "url": link
                    }
                    logistics_collection.update_one(
                        {"title": title, "location": "General", "est_datetime": est_time},
                        {"$set": logistics_doc},
                        upsert=True
                    )
                    logger.info(f"Stored general logistics data: {title[:50]}...")
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