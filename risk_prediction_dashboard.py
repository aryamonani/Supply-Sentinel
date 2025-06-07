#!/usr/bin/env python3

import streamlit as st
import pandas as pd
from pymongo import MongoClient
import pytz
from datetime import datetime, timedelta
import logging
import requests
import base64
import time
import os
from dotenv import load_dotenv
import google.generativeai as genai
from geopy.distance import geodesic
import re
import requests.utils
import plotly.express as px # Import Plotly Express

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DEBUGGING .ENV LOADING ---
import os.path
_dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
print(f"DEBUG: Attempting to load .env from: {_dotenv_path}")
print(f"DEBUG: .env file exists at path: {os.path.exists(_dotenv_path)}")
print(f"DEBUG: ATLAS_PUBLIC_KEY BEFORE load_dotenv: '{os.environ.get('ATLAS_PUBLIC_KEY')}'")
# --- END DEBUGGING .ENV LOADING ---

# Load environment variables from .env file
load_dotenv()

# --- DEBUGGING .ENV LOADING (CONTINUED) ---
print(f"DEBUG: ATLAS_PUBLIC_KEY AFTER load_dotenv: '{os.environ.get('ATLAS_PUBLIC_KEY')}'")
# --- END DEBUGGING .ENV LOADING (CONTINUED) ---


# Configure logging (already here, kept for consistency)
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__) # Re-using existing logger instance

# --- DEBUG PRINT FOR GEMINI API KEY (already here, kept for consistency) ---
print(f"DEBUG: Raw GEMINI_API_KEY from os.environ: '{os.environ.get('GEMINI_API_KEY')}'")
# --- END DEBUG PRINT ---

# --- Custom CSS for Purity UI Dashboard Inspiration ---
st.markdown("""
<style>
/* Overall app background - Cream Gradient */
.stApp {
    background: linear-gradient(to right, #FFFACD, #FDF5E6); /* Cream to very light yellow-white */
    color: black; /* Set general text color to black for readability on light background */
}

/* Sidebar styling for frosted glass effect */
[data-testid="stSidebar"] {
    background: rgba(255, 255, 255, 0.2); /* Semi-transparent white */
    backdrop-filter: blur(10px); /* Frosted glass effect */
    -webkit-backdrop-filter: blur(10px); /* For Safari */
    border-right: 1px solid rgba(255, 255, 255, 0.1); /* Subtle border */
}
[data-testid="stSidebar"] * {
    color: black; /* General text color for sidebar */
}
[data-testid="stSidebar"] p, [data-testid="stSidebar"] label {
    color: black !important; /* More specific for labels/text */
}

/* --- REVISED AND MORE SPECIFIC RULES FOR SELECTBOX --- */

/* Target the container of the selectbox to override its theme background */
[data-testid="stSidebar"] [data-testid="stSelectbox"] {
    background-color: white !important;
    border-radius: 8px;
    padding: 2px; /* Add a little padding to show the white container */
}

/* Target the clickable part of the selectbox */
[data-testid="stSidebar"] [data-testid="stSelectbox"] div[data-baseweb="select"] {
    background-color: white !important;
    border: 1px solid #ccc !important;
    border-radius: 5px !important;
}

/* Target the text inside the clickable part */
[data-testid="stSidebar"] [data-testid="stSelectbox"] div[data-baseweb="select"] > div {
    background-color: transparent !important; /* Make inner div transparent */
    color: black !important;
}

/* Target the dropdown arrow icon */
[data-testid="stSidebar"] [data-testid="stSelectbox"] svg {
    fill: black !important;
}

/* Target the pop-up menu that appears on click */
div[data-baseweb="popover"] ul[role="listbox"] {
    background-color: white !important;
    border: 1px solid #ccc !important;
    border-radius: 5px !important;
}

/* Target individual list items in the pop-up menu */
div[data-baseweb="popover"] ul[role="listbox"] li {
    color: black !important;
    background-color: white !important;
}

/* Target the hover effect on list items */
div[data-baseweb="popover"] ul[role="listbox"] li:hover {
    background-color: #f0f0f0 !important;
    color: black !important;
}


/* General Card Styling */
.stContainer {
    background-color: white;
    border-radius: 10px;
    box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.05);
    padding: 20px;
    margin-bottom: 20px;
}
div[data-testid="stMetric"] {
    background-color: white;
    border-radius: 10px;
    box-shadow: 0 2px 4px 0 rgba(0, 0, 0, 0.03);
    padding: 15px;
    margin-bottom: 10px;
    text-align: center;
    color: black;
}
div[data-testid="stMetricLabel"] p {
    font-size: 1rem;
    color: #666;
    margin-bottom: 5px;
}
div[data-testid="stMetricValue"] {
    font-size: 2rem;
    font-weight: bold;
    color: #333;
}
.stDataFrame table { 
  width: 100% !important;
  table-layout: auto !important;
  min-width: 100%;
  background-color: white !important;
  border-collapse: collapse;
}
.stDataFrame table th, .stDataFrame table td {
  white-space: normal !important;
  word-wrap: break-word !important;
  padding: 12px 8px;
  border: 1px solid #eee;
  color: black !important;
}
.stDataFrame table th {
  background-color: #f8f9fa !important;
  color: #333 !important;
  text-align: left;
  font-weight: bold;
}
a {
    color: #007bff;
    text-decoration: none;
}
a:hover {
    text-decoration: underline;
    color: #0056b3;
}
.stButton>button {
    background-color: white !important;
    color: black !important;
    border: 1px solid #ddd !important;
    border-radius: 5px !important;
    box-shadow: 0 2px 4px 0 rgba(0,0,0,0.05) !important;
}
.stButton>button:hover {
    background-color: #f0f0f0 !important;
    border-color: #ccc !important;
}
.stBlock {
    color: black; 
}
p {
    color: black;
}
h1, h2, h3, h4, h5, h6 {
    color: #333;
}
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
header { visibility: hidden; } 
</style>
""", unsafe_allow_html=True)

# Retrieve credentials from environment variables
ATLAS_PUBLIC_KEY = os.environ.get("ATLAS_PUBLIC_KEY")
ATLAS_PRIVATE_KEY = os.environ.get("ATLAS_PRIVATE_KEY")
PROJECT_ID = os.environ.get("PROJECT_ID")
MONGO_URI = os.environ.get("MONGO_URI")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# Check if all required environment variables are set
required_vars = ["ATLAS_PUBLIC_KEY", "ATLAS_PRIVATE_KEY", "PROJECT_ID", "MONGO_URI", "GEMINI_API_KEY"]
for var in required_vars:
    if not os.environ.get(var):
        st.error(f"Missing environment variable: {var}. Please set it in the .env file.")
        st.stop()

# Function to get the user's public IP
def get_public_ip():
    try:
        response = requests.get("https://ifconfig.me", timeout=5)
        response.raise_for_status()
        public_ip = response.text.strip()
        logger.info(f"Public IP fetched: {public_ip}")
        return public_ip
    except Exception as e:
        logger.error(f"Failed to fetch public IP: {str(e)}")
        return None

# MongoDB Setup with Retry Logic
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
    st.error(f"Database connection failed: {str(e)}")
    st.stop()

# Database Setup
DB_NAME = "supply_chain_db"
db = client[DB_NAME]
inventory_collection = db["inventory"]
shipments_collection = db["shipments"]
fulfillment_centers_collection = db["fulfillment_centers"]
gemini_prompts_collection = db["gemini_prompts"]
weather_collection = db["weather"]
social_media_collection = db["social_media"]
news_collection = db["news"]
labor_collection = db["labor"]
logistics_collection = db["logistics"]

# Helper function to get EST datetime
def get_est_datetime():
    est = pytz.timezone("America/New_York")
    return datetime.now(est).isoformat()

# Fetch FCs dynamically from database with coordinates
def get_fcs():
    try:
        # Fetch new FC fields for re-routing calculations
        fc_docs = list(fulfillment_centers_collection.find({}, {"FC_Name": 1, "FC_ID": 1, "city": 1, "Latitude": 1, "Longitude": 1, "re_routing_cost_multiplier": 1, "re_routing_tat_adder_days": 1}))
        if not fc_docs:
            logger.warning("No FCs found in the database.")
            st.warning("No Fulfillment Centers found in the database. Please ensure data is populated in MongoDB.")
            return [], {}, {}, {}, {}
        fc_to_city = {doc["FC_Name"]: doc["city"] for doc in fc_docs}
        fc_to_fc_id = {doc["FC_Name"]: doc["FC_ID"] for doc in fc_docs}
        fc_id_to_name = {doc["FC_ID"]: doc["FC_Name"] for doc in fc_docs}
        # Store re-routing details in fc_coordinates for easier lookup
        fc_coordinates = {
            doc["FC_ID"]: {
                "coords": (doc["Latitude"], doc["Longitude"]),
                "cost_multiplier": doc.get("re_routing_cost_multiplier", 1.2), # Default if not found
                "tat_adder": doc.get("re_routing_tat_adder_days", 1) # Default if not found
            } for doc in fc_docs
        }
        logger.info(f"Fetched {len(fc_docs)} FCs from the database: {fc_to_city.keys()}")
        return list(fc_to_city.keys()), fc_to_city, fc_to_fc_id, fc_id_to_name, fc_coordinates
    except Exception as e:
        logger.error(f"Error fetching FCs: {str(e)}")
        st.error(f"Error fetching FCs: {str(e)}")
        return [], {}, {}, {}, {}

# Nearby Cities for Re-routing (unchanged)
nearby_cities = {
    "New York": ["Newark", "Jersey City", "Paterson", "Philadelphia"],
    "Buffalo": ["Rochester", "Pittsburgh"],
    "Rochester": ["Buffalo", "Pittsburgh"],
    "Newark": ["New York", "Jersey City", "Paterson", "Philadelphia"],
    "Jersey City": ["New York", "Newark", "Paterson", "Philadelphia"],
    "Paterson": ["New York", "Newark", "Jersey City", "Philadelphia"],
    "Philadelphia": ["New York", "Newark", "Jersey City", "Allentown"],
    "Pittsburgh": ["Buffalo", "Rochester"],
    "Allentown": ["Philadelphia"],
    "Boston": ["Worcester", "Springfield"],
    "Worcester": ["Boston", "Springfield"],
    "Springfield": ["Boston", "Worcester"],
    "Baltimore": ["Silver Spring", "Frederick", "Washington"],
    "Silver Spring": ["Baltimore", "Frederick", "Washington"],
    "Frederick": ["Baltimore", "Silver Spring", "Washington"],
    "Washington": ["Baltimore", "Silver Spring", "Frederick"],
    "Seattle": [],
    "Houston": [],
    "Los Angeles": [],
    "Miami": [],
    "Denver": [],
    "Chicago": [],
    "Atlanta": [],
    "Phoenix": [],
    "Dallas": []
}

# Scenarios for Simulation Mode
scenarios = {
    "Hurricane in Houston": {
        "description": "A Category 4 hurricane hits Houston, shutting down the main FC for 3 days. Roads are blocked, and 50% of inventory is damaged.",
        "affected_cities": ["Houston"],
        "event_type": "weather",
        "severity": "high"
    },
    "Earthquake in Los Angeles": {
        "description": "A 6.5 magnitude earthquake disrupts LA operations for 2 weeks. Minor structural damage to the warehouse; 20% workforce unavailable.",
        "affected_cities": ["Los Angeles"],
        "event_type": "weather",
        "severity": "moderate"
    },
    "Flooding in Miami": {
        "description": "Seasonal flooding impacts Miami roads for 1-5 days. Delivery delays expected; no damage to inventory.",
        "affected_cities": ["Miami"],
        "event_type": "weather",
        "severity": "low"
    },
    "Wildfire in Denver": {
        "description": "Wildfires approach Denver, with evacuation warnings issued 48 hours in advance. Air quality halts operations; potential closure for 1 week.",
        "affected_cities": ["Denver"],
        "event_type": "weather",
        "severity": "high"
    },
    "Strike in Chicago": {
        "description": "Workers strike at the Chicago FC for 5 days. 80% reduction in operational capacity.",
        "affected_cities": ["Chicago"],
        "event_type": "labor",
        "severity": "high"
    },
    "Sick-Out in Seattle": {
        "description": "A sudden illness affects 30% of Seattle staff, with recovery unpredictable (2-7 days). Reduced picking and packing efficiency.",
        "affected_cities": ["Seattle"],
        "event_type": "labor",
        "severity": "low"
    },
    "Union Negotiation Delay in New York": {
        "description": "Ongoing union talks threaten a 2-week slowdown starting in 10 days. 50% productivity drop anticipated.",
        "affected_cities": ["New York"],
        "event_type": "labor",
        "severity": "moderate"
    },
    "Supplier Failure in Atlanta": {
        "description": "A key supplier in Atlanta goes bankrupt, halting deliveries for 1 week. 30% of critical inventory unavailable.",
        "affected_cities": ["Atlanta"],
        "event_type": "inventory",
        "severity": "high"
    },
    "Inventory Spoilage in Phoenix": {
        "description": "A refrigeration failure spoils 25% of perishable goods in Phoenix. Immediate loss with no warning.",
        "affected_cities": ["Phoenix"],
        "event_type": "inventory",
        "severity": "moderate"
    },
    "Overstock in Dallas": {
        "description": "A forecasting error leads to 40% excess inventory in Dallas for 1 month. Storage costs rise; potential for obsolescence.",
        "affected_cities": ["Dallas"],
        "event_type": "inventory",
        "severity": "low"
    },
    "Simultaneous Storms in Miami and Houston": {
        "description": "Tropical storms hit both Miami and Houston, closing centers for 4 days each. Cross-regional shipping disrupted.",
        "affected_cities": ["Miami", "Houston"],
        "event_type": "weather",
        "severity": "high"
    },
    "Nationwide Trucking Strike": {
        "description": "Truck drivers strike across the US for 3-10 days. All ground transport delayed; air freight costs spike.",
        "affected_cities": ["New York", "Buffalo", "Rochester", "Newark", "Jersey City", "Paterson", "Philadelphia",
                           "Pittsburgh", "Allentown", "Boston", "Worcester", "Springfield", "Baltimore",
                           "Silver Spring", "Frederick"], # All locations with FCs
        "event_type": "labor",
        "severity": "high"
    },
    "Power Outage Across Northeast": {
        "description": "A grid failure affects New York, Boston, and Philadelphia for 2 days. Backup generators fail in 1 out of 3 centers (randomize).",
        "affected_cities": ["New York", "Boston", "Philadelphia"],
        "event_type": "weather",
        "severity": "moderate"
    },
    "Volcanic Eruption in Seattle": {
        "description": "A rare volcanic eruption disrupts Seattle for 3 weeks. Air and ground transport halted; 70% inventory inaccessible.",
        "affected_cities": ["Seattle"],
        "event_type": "weather",
        "severity": "extreme"
    },
    "Cyber Attack in Chicago": {
        "description": "A ransomware attack locks systems with 1-hour notice. Unknown downtime (2-5 days); potential data loss.",
        "affected_cities": ["Chicago"],
        "event_type": "other",
        "severity": "high"
    },
    "Regulatory Change in Los Angeles": {
        "description": "New emissions rules ban 50% of delivery trucks starting in 30 days. Long-term operational shift required.",
        "affected_cities": ["Los Angeles"],
        "event_type": "other",
        "severity": "low"
    },
    "Heatwave in Phoenix": {
        "description": "Temperatures exceed 110°F for 5 days, with 10-50% staff absenteeism (randomize). Reduced throughput; potential equipment failure.",
        "affected_cities": ["Phoenix"],
        "event_type": "weather",
        "severity": "variable"
    },
    "Customs Delay in New York": {
        "description": "An international shipment is held for 1-3 days (randomize lead time). 20% of incoming inventory delayed.",
        "affected_cities": ["New York"],
        "event_type": "inventory",
        "severity": "variable"
    },
    "Competitor Disruption Affecting Chicago": {
        "description": "A competitor’s warehouse fire floods Chicago with redirected orders. 30% demand surge for 1 week.",
        "affected_cities": ["Chicago"],
        "event_type": "other",
        "severity": "low"
    },
    "Pandemic Wave Across All Centers": {
        "description": "A new health crisis reduces staff by 20-40% across all locations for 1 month. Randomize absenteeism per center; shipping delays increase.",
        "affected_cities": ["New York", "Buffalo", "Rochester", "Newark", "Jersey City", "Paterson", "Philadelphia",
                           "Pittsburgh", "Allentown", "Boston", "Worcester", "Springfield", "Baltimore",
                           "Silver Spring", "Frederick"], # All locations with FCs
        "event_type": "labor",
        "severity": "extreme"
    },
    # Additional Test Cases: Nearest FC Found but Inventory Not Available (rely on data generation + logic)
    "Nearest FC Lacks Inventory (Specific)": { # Example to highlight inventory issue
        "description": "Local disruption, triggering a need for emergency SKUs. The nearest FC to the shipment's destination has only 50% of the required inventory.",
        "affected_cities": ["Houston"], # Or any city where you can manually set data for testing
        "event_type": "inventory",
        "severity": "high",
        "affected_fcs": ["Houston FC 1"] # Example: assuming Houston FC 1 has emergency items that are low in stock
    },
    "No Nearby FCs Have Sufficient Inventory (Specific)": { # Example
        "description": "Major disruption, no FC within 150 miles has >=90% required inventory for emergency SKUs.",
        "affected_cities": ["Los Angeles"], # Or any city where you can manually set data for testing
        "event_type": "inventory",
        "severity": "moderate",
        "affected_fcs": ["Los Angeles FC X"]
    },
    "Nearest FC Has Partial Inventory (Specific)": { # Example
        "description": "Minor disruption, nearest FC has 80% required inventory for emergency SKUs (below 90% threshold).",
        "affected_cities": ["Miami"], # Or any city where you can manually set data for testing
        "event_type": "inventory",
        "severity": "low",
        "affected_fcs": ["Miami FC X"]
    },
    "Nearest FC Is Outside 150-Mile Radius (Specific)": { # Example
        "description": "Disruption isolates area; nearest FC with sufficient inventory is >150 miles away.",
        "affected_cities": ["Denver"], # Or any city where you can manually set data for testing
        "event_type": "weather",
        "severity": "high",
        "affected_fcs": ["Denver FC X"]
    },
     "Multiple FCs Varying Inventory Levels (Specific)": { # Example
        "description": "Disruption affects inventory; nearest FC has 85% inventory, next has 95%, farther has 100%.",
        "affected_cities": ["Chicago"], # Or any city where you can manually set data for testing
        "event_type": "inventory",
        "severity": "high",
        "affected_fcs": ["Chicago FC X"]
    }
}


# Simulated Data Functions
def get_simulated_weather(scenario, city):
    if scenario["event_type"] == "weather":
        return [
            {
                "est_datetime": get_est_datetime(),
                "weather": scenario["description"].lower(),
                "temp": 25 if "hurricane" in scenario["description"].lower() else 20,
                "description": f"{scenario['severity'].capitalize()} severity {scenario['event_type']}"
            }
        ] * 5
    return []

def get_simulated_social_media(scenario, city):
    # This function now handles both 'weather' and 'labor' event types
    # and also general 'other' event types if their description hints at a social media impact
    if scenario["event_type"] in ["weather", "labor"] or ("other" in scenario["event_type"] and any(word in scenario["description"].lower() for word in ["protest", "cyber", "public reaction"])):
        return [
            {
                "subreddit": "simulation",
                "created_utc": get_est_datetime(),
                "text": f"Simulated {scenario['event_type']} event in {city}: {scenario['description']}",
                "sentiment": "negative" if scenario["severity"] == "high" else "neutral"
            }
        ] * 5
    return []

def get_simulated_inventory(scenario, fc, fc_id):
    # This function is triggered if event_type is 'inventory' OR if 'affected_fcs' is specified for inventory manipulation
    if scenario["event_type"] == "inventory" and fc in scenario.get("affected_fcs", []):
        inventory_docs = list(inventory_collection.find({"FC_ID": fc_id}))
        # Apply specific simulation logic based on scenario description if needed
        if "50% of inventory is damaged" in scenario["description"].lower():
            for doc in inventory_docs:
                doc["Quantity"] = doc.get("Quantity", 1) * 0.5 # 50% damage
        elif "20% workforce unavailable" in scenario["description"].lower():
             # Workforce affects throughput, not direct inventory damage.
             # For inventory impact, we might set quantity to 0 or a low number for some SKUs
            pass # No direct inventory change for this scenario type here.
        elif "30% of critical inventory unavailable" in scenario["description"].lower():
             for doc in inventory_docs:
                 if doc.get("L1_Category") in ["Health & Household", "Industrial & Scientific"]: # Example of critical category
                     doc["Quantity"] = doc.get("Quantity", 1) * 0.7 # 30% unavailable
        elif "refrigeration failure spoils 25% of perishable goods" in scenario["description"].lower():
            for doc in inventory_docs:
                if doc.get("L1_Category") == "Grocery & Gourmet Food": # Example of perishable category
                    doc["Quantity"] = doc.get("Quantity", 1) * 0.75 # 25% spoiled
        elif "40% excess inventory" in scenario["description"].lower():
             for doc in inventory_docs:
                 doc["Quantity"] = doc.get("Quantity", 1) * 1.4 # 40% excess
        elif "20% of incoming inventory delayed" in scenario["description"].lower():
             # Delay affects incoming, not current physical inventory.
             # To simulate, might reduce some current inventory or flag as 'unavailable'
            pass
        elif "nearest fc lacks inventory" in scenario["description"].lower():
            # This specific scenario needs a very precise setup in dynamic_data_generation.py
            # or manual DB intervention to work correctly.
            # Here, we can simulate *some* reduction to ensure it triggers the logic
            for doc in inventory_docs:
                if doc.get("Is_Emergency_Defined"): # Target emergency products
                    doc["Quantity"] = doc.get("Quantity", 1) * 0.5 # Reduce to simulate lack
        elif "no nearby fcs have sufficient inventory" in scenario["description"].lower():
             # Similar to above, reduce many FCs' inventory
            for doc in inventory_docs:
                if doc.get("Is_Emergency_Defined"):
                    doc["Quantity"] = doc.get("Quantity", 1) * 0.3 # Simulate widespread low stock
        elif "nearest fc has partial inventory" in scenario["description"].lower():
             for doc in inventory_docs:
                 if doc.get("Is_Emergency_Defined"):
                     doc["Quantity"] = doc.get("Quantity", 1) * 0.8 # Simulate 80% availability
        elif "multiple fcs varying inventory levels" in scenario["description"].lower():
            # This is too complex for a generic simulated function. Needs careful manual setup.
            pass

        return inventory_docs
    return []

# Gemini Prediction Function
def gemini_predict(prompt, fc_name="Unknown FC"):
    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY is not available for Gemini API configuration.")
        return 50, "Unknown", "GEMINI_API_KEY not found.", []

    genai.configure(api_key=GEMINI_API_KEY)
    
    try:
        list_models_response = genai.list_models()
        available_models = [
            m.name for m in list_models_response
            if 'generateContent' in m.supported_generation_methods and "vision" not in m.name and "image-generation" not in m.name
        ]
        logger.info(f"Available Gemini models supporting generateContent (text only): {available_models}")

        model_name = None
        priority_models_candidates = [
            "gemini-1.5-pro-latest", "models/gemini-1.5-pro-latest",
            "gemini-1.5-pro-002", "models/gemini-1.5-pro-002",
            "gemini-1.5-pro", "models/gemini-1.5-pro",
            "gemini-1.5-flash-latest", "models/gemini-1.5-flash-latest",
            "gemini-1.5-flash-002", "models/gemini-1.5-flash-002",
            "gemini-1.5-flash", "models/gemini-1.5-flash",
            "gemini-1.0-pro-latest", "models/gemini-1.0-pro-latest",
            "gemini-1.0-pro-001", "models/gemini-1.0-pro-001",
            "gemini-1.0-pro", "models/gemini-1.0-pro",
            "gemini-pro", "models/gemini-pro",
            "gemini-2.5-pro-preview-06-05", "models/gemini-2.5-pro-preview-06-05",
            "gemini-2.0-flash", "models/gemini-2.0-flash",
        ]
        
        for p_model in priority_models_candidates:
            if p_model in available_models:
                model_name = p_model
                break

        if not model_name:
            logger.error("No suitable Gemini model found that supports generateContent from the priority list.")
            return 50, "Unknown", "No suitable Gemini model found.", []

        model = genai.GenerativeModel(model_name)
        logger.info(f"Using Gemini model: {model_name}")

    except Exception as e:
        logger.error(f"Error listing or selecting Gemini models: {str(e)}")
        return 50, "Unknown", f"Error listing or selecting Gemini models: {str(e)}", []

    try:
        response = model.generate_content(prompt)
        generated_text = response.text
        
        logger.info(f"Raw Gemini Output for FC: {fc_name}:\n{generated_text}")

        risk_score = 50
        status = "Unknown"
        reasoning = ""
        emergency_classifications = []

        # Parse Risk Score
        risk_score_match = re.search(r"Risk Score:\s*(\d+\.?\d*)", generated_text)
        if risk_score_match:
            risk_score = float(risk_score_match.group(1))

        # Parse Status
        status_match = re.search(r"Status:\s*(.+)", generated_text)
        if status_match:
            status = status_match.group(1).strip()

        # Parse Reasoning (more robustly)
        reasoning_section_match = re.search(r"Reasoning:\s*(.*?)(?=\nEmergency Classifications:|\Z)", generated_text, re.DOTALL)
        if reasoning_section_match:
            reasoning = reasoning_section_match.group(1).strip()
        else:
            reasoning = ""

        # Parse Emergency Classifications
        sku_matches = re.findall(r"-\s*SKU:\s*([^,]+),\s*Emergency:\s*(True|False),\s*Reason:\s*(.+)", generated_text)
        for match in sku_matches:
            sku = match[0].strip()
            emergency = match[1].strip() == "True"
            reason = match[2].strip()
            emergency_classifications.append({"SKU": sku, "Emergency": emergency, "Reason": reason})
        
        logger.info("Successfully received response from Gemini API")
        return risk_score, status, reasoning, emergency_classifications
    
    except Exception as e:
        logger.error(f"Gemini prediction error with model {model_name}: {str(e)}")
        return 50, "Unknown", f"Gemini prediction error: {str(e)}", []

# Generate Risk Prompt for Gemini
def generate_risk_prompt(fc_name, city, simulated_weather=None, simulated_social_media=None, simulated_inventory=None):
    current_time = time.time()
    time_threshold = current_time - 86400  # 24 hours ago
    
    prompt = f"""
    You are an AI expert in supply chain risk management for Amazon Fulfillment Centers (FCs). Your task is to:
    1. Assess the risk of disruption for the {fc_name} located in {city} based on the provided data.
    2. Determine if products in the FC's inventory belong to emergency categories critical for public health and safety.
    3. Provide a risk score (0-100), status, reasoning, and emergency category classifications.

    ### Data for {fc_name} ({city})
    #### Weather Data (Last 24 Hours)
    """
    # --- MODIFIED: Ensure simulated_weather is used if provided, else fetch from DB ---
    weather_docs = simulated_weather if simulated_weather is not None else list(
        weather_collection.find({"location": city, "timestamp": {"$gte": time_threshold}}).sort("timestamp", -1).limit(50)
    )
    if not weather_docs:
        prompt += "No recent weather data available.\n"
    else:
        for doc in weather_docs:
            prompt += f"- {doc.get('est_datetime', 'N/A')}: Weather {doc.get('weather', 'N/A')}, Temp {doc.get('temp', 'N/A')}°C, Conditions: {doc.get('description', 'N/A')}\n"
    
    prompt += """
    #### Social Media (Reddit, Last 24 Hours)
    """
    # --- MODIFIED: Ensure simulated_social_media is used if provided, else fetch from DB ---
    reddit_docs = simulated_social_media if simulated_social_media is not None else list(
        social_media_collection.find({"location": city, "timestamp": {"$gte": time_threshold}}).sort("timestamp", -1)
    )
    if not reddit_docs:
        prompt += "No recent social media data available.\n"
    else:
        for doc in reddit_docs:
            post_text = doc.get('text', 'N/A')
            subreddit = doc.get('subreddit', 'N/A')
            created_utc = doc.get('created_utc', 'N/A')
            sentiment = doc.get('sentiment', 'Neutral')
            
            post_url = doc.get('url') or doc.get('permalink') # Check for direct URL
            
            link_text = ""
            if post_url:
                link_text = f" [Link]({post_url})"
            elif post_text != 'N/A': # If no direct URL, offer a search link
                search_query = requests.utils.quote(post_text) # URL-encode the text
                if subreddit != 'N/A':
                    search_url = f"https://www.reddit.com/r/{subreddit}/search?q={search_query}&restrict_sr=on&sort=new&t=day"
                else:
                    search_url = f"https://www.reddit.com/search?q={search_query}&sort=new&t=day"
                link_text = f" [Search Reddit]({search_url})"
                
            prompt += f"- r/{subreddit} ({created_utc}): \"{post_text}\" (Sentiment: {sentiment}){link_text}\n"
    
    prompt += """
    #### News (Last 24 Hours)
    """
    news_docs = list(news_collection.find({"location": city, "timestamp": {"$gte": time_threshold}}).sort("timestamp", -1))
    if not news_docs:
        prompt += "No recent news data available.\n"
    else:
        for doc in news_docs:
            prompt += f"- Reuters ({doc.get('timestamp', 'N/A')}): \"{doc.get('description', 'N/A')}\" (Impact: {doc.get('impact', 'Unknown')})\n"
    
    prompt += """
    #### Labor (Last 24 Hours)
    """
    labor_docs = list(labor_collection.find({"location": city, "timestamp": {"$gte": time_threshold}}).sort("timestamp", -1))
    if not labor_docs:
        prompt += "No recent labor data available.\n"
    else:
        for doc in labor_docs:
            prompt += f"- Reuters ({doc.get('timestamp', 'N/A')}): \"{doc.get('description', 'N/A')}\" (Severity: {doc.get('severity', 'Unknown')})\n"
    
    prompt += """
    #### Logistics (Last 24 Hours)
    """
    logistics_docs = list(logistics_collection.find({"location": city, "timestamp": {"$gte": time_threshold}}).sort("timestamp", -1))
    if not logistics_docs:
        prompt += "No recent logistics data available.\n"
    else:
        for doc in logistics_docs:
            prompt += f"- FreightWaves ({doc.get('est_datetime', 'N/A')}): \"{doc.get('description', 'N/A')}\" (Disruption Level: {doc.get('disruption_level', 'Unknown')})\n"
    
    prompt += """
    #### Inventory (All Products)
    """
    # --- MODIFIED: Ensure simulated_inventory is used if provided, else fetch from DB ---
    # Note: fc_to_fc_id may not be directly available if simulated_inventory is the only data source.
    # We should ensure this path is robust for simulation.
    if simulated_inventory is not None:
        inventory_docs = simulated_inventory
    else:
        # Assuming fc_name is available here from the loop for Real Mode
        fc_id = fc_to_fc_id[fc_name]
        inventory_docs = list(inventory_collection.find({"FC_ID": fc_id}))

    if not inventory_docs:
        prompt += "No inventory data available.\n"
    else:
        for doc in inventory_docs:
            prompt += f"- SKU: {doc.get('Product_SKU', 'N/A')}, Category: {doc.get('L1_Category', 'N/A')}, Description: {doc.get('Product_Description', 'N/A')}, Quantity: {doc.get('Quantity', 'N/A')}\n"
    
    prompt += """
    ### Instructions
    1. **Risk Assessment**:
        - Analyze the data to predict the risk of disruption (0-100).
        - Explain your reasoning for the risk score.

    2. **Emergency Category Classification**:
        - For each product, determine if it’s an emergency item based on category and description.
        - Output SKUs with emergency status (True/False) and reasoning.

    3. **Output Format**:
    - Risk Score: [number]
    - Status: [string]
    - Reasoning: [text]
    - Emergency Classifications:
      - SKU: [sku], Emergency: [True/False], Reason: [text]
    """
    return prompt

# Check Inventory Availability
def check_inventory(fc_id, sku, required_qty):
    inventory_doc = inventory_collection.find_one({"FC_ID": fc_id, "Product_SKU": sku})
    if inventory_doc and "Quantity" in inventory_doc:
        available_qty = inventory_doc["Quantity"]
        return (available_qty / required_qty) * 100 if required_qty > 0 else 0.0
    return 0.0

# Function to get nearest FCs based on distance
def get_nearest_fcs(destination_lat, destination_lon, fc_coordinates): # Removed default max_distance
    nearest_fcs = []
    # fc_coordinates now holds details like cost_multiplier, tat_adder
    for fc_id, fc_details in fc_coordinates.items():
        fc_lat, fc_lon = fc_details["coords"]
        distance = geodesic((destination_lat, destination_lon), (fc_lat, fc_lon)).miles
        if distance <= 150: # Using fixed 150 miles for now
            nearest_fcs.append((fc_id, distance))
    nearest_fcs.sort(key=lambda x: x[1])  # Sort by distance
    return [fc[0] for fc in nearest_fcs]


# Updated Contingency Plan Logic
def generate_contingency_plan(fc_name, city, risk_score, risk_data, fc_coordinates, current_emergency_classifications, shipments_collection, fulfillment_centers_collection):
    full_contingency_plan = []
    
    emergency_skus = [c["SKU"] for c in current_emergency_classifications if c["Emergency"]]
    
    summary_status = "No re-routing needed"
    
    # Define conditions for re-routing evaluation
    should_evaluate_rerouting = False
    gemini_status_lower = risk_data.get(fc_name, {}).get("Status", "Unknown").lower()
    reasoning_lower = risk_data.get(fc_name, {}).get("Reasoning", "").lower()

    if "high risk" in gemini_status_lower:
        should_evaluate_rerouting = True
        full_contingency_plan.append(f"Gemini assessed status as **High Risk**. Immediate re-routing evaluation triggered.")
    elif "medium risk" in gemini_status_lower:
        should_evaluate_rerouting = True
        full_contingency_plan.append(f"Gemini assessed status as **Medium Risk**. Re-routing evaluation for potential disruptions triggered.")
    # Condition to catch if Gemini identified emergency SKUs and mentioned any disruption/risk keyword
    elif emergency_skus and any(word in reasoning_lower for word in ["disruption", "delay", "impact", "traffic", "issue", "risk"]):
        should_evaluate_rerouting = True
        full_contingency_plan.append(f"Emergency SKUs detected and disruption/risk mentioned in reasoning. Re-routing evaluation triggered.")
    # Fallback to numerical threshold if no textual risk status triggers it (e.g., Gemini returns "Low Risk" but score is high)
    elif risk_score > 30: # Threshold for basic risk
        should_evaluate_rerouting = True
        full_contingency_plan.append(f"Risk score ({risk_score}) is above numerical threshold (30). Evaluating re-routing for affected shipments.")
            
    if not should_evaluate_rerouting:
        full_contingency_plan.append("Gemini assessed low risk and no emergency-specific triggers. No specific conditions met for re-routing evaluation.")
        return summary_status, "\n".join(full_contingency_plan), [] # Return empty list for chart data

    # If we reached here, evaluation is needed
    if not emergency_skus:
        full_contingency_plan.append("No emergency SKUs identified for re-routing consideration. (General re-routing for non-emergency items not implemented)")
        return "Re-routing evaluation needed (No Emergency SKUs)", "\n".join(full_contingency_plan), [] # Return empty list for chart data

    # The actual re-routing logic for emergency SKUs:
    shipments_found_for_emergency_skus = False
    re_routing_options_found = False
    
    # Track status for emergency SKUs for the pie chart
    emergency_sku_reroute_status = [] # List of {"SKU": "...", "Status": "Rerouted"|"No shipment"|"No optimal route"}

    for sku in emergency_skus:
        # Fetch active shipments for this SKU
        shipments = list(shipments_collection.find({"Product_SKU": sku, "Status": {"$in": ["Pending", "In Transit", "Out for Delivery"]}}))
        
        if not shipments:
            full_contingency_plan.append(f"No active shipments found for emergency SKU {sku}.")
            emergency_sku_reroute_status.append({"SKU": sku, "Status": "No Active Shipment"})
            continue
        else:
            shipments_found_for_emergency_skus = True

        found_route_for_this_sku = False
        for shipment in shipments:
            shipment_id = shipment.get("Shipment_ID", "N/A")
            required_qty = shipment.get("Order_Volume", 0)
            dest_lat = shipment.get("Destination_Lat")
            dest_lon = shipment.get("Destination_Lon")
            original_cost = shipment.get("initial_shipping_cost", 0)
            original_tat_days = shipment.get("initial_delivery_tat_days", 0)
            
            if dest_lat is None or dest_lon is None:
                full_contingency_plan.append(f"Shipment {shipment_id} ({sku}): Missing destination coordinates.")
                emergency_sku_reroute_status.append({"SKU": sku, "Status": "No Optimal Route"}) # Classify as no optimal route if coords missing
                found_route_for_this_sku = True # Mark as "handled" for this SKU
                break # Move to next SKU if coords are bad

            # Find nearest FCs with capacity
            nearest_fcs_ids = get_nearest_fcs(dest_lat, dest_lon, fc_coordinates)
            
            found_alternative_for_this_shipment = False
            for nearby_fc_id in nearest_fcs_ids:
                nearby_fc_name = fc_id_to_name.get(nearby_fc_id, nearby_fc_id) # Get name from global mapping
                # Fetch detailed FC info including multipliers
                nearby_fc_details_doc = fulfillment_centers_collection.find_one({"FC_ID": nearby_fc_id}) 
                
                if not nearby_fc_details_doc:
                    logger.warning(f"FC details not found in DB for nearby_fc_id: {nearby_fc_id}")
                    continue

                cost_multiplier = nearby_fc_details_doc.get("re_routing_cost_multiplier", 1.2)
                tat_adder = nearby_fc_details_doc.get("re_routing_tat_adder_days", 1)

                availability = check_inventory(nearby_fc_id, sku, required_qty)
                if availability >= 90: # If sufficient inventory is available
                    re_routed_cost = original_cost * cost_multiplier
                    re_routed_tat_days = original_tat_days + tat_adder
                    
                    cost_increase = re_routed_cost - original_cost
                    tat_delay_days = re_routed_tat_days - original_tat_days

                    full_contingency_plan.append(
                        f"Shipment {shipment_id} ({sku}): **Re-route to {nearby_fc_name}** "
                        f"(Inv: {availability:.1f}%) | "
                        f"Original Cost: ${original_cost:.2f}, New Cost: ${re_routed_cost:.2f}, **Cost Δ: ${cost_increase:.2f}** | "
                        f"Original TAT: {original_tat_days} days, New TAT: {re_routed_tat_days} days, **TAT Δ: {tat_delay_days} days**"
                    )
                    re_routing_options_found = True
                    found_alternative_for_this_shipment = True
                    emergency_sku_reroute_status.append({"SKU": sku, "Status": "Re-routed"})
                    break # Found a route for this shipment, move to next shipment for this SKU
            
            if not found_alternative_for_this_shipment:
                full_contingency_plan.append(f"Shipment {shipment_id} ({sku}): No nearby FC with sufficient inventory within 150 miles to re-route.")
                emergency_sku_reroute_status.append({"SKU": sku, "Status": "No Optimal Route"}) # Specific status if no alternative found within radius

    if re_routing_options_found:
        summary_status = "Re-routing options available"
    elif shipments_found_for_emergency_skus:
        summary_status = "Re-routing evaluation: No optimal routes found"
    else:
        summary_status = "No active emergency shipments found"

    return summary_status, "\n".join(full_contingency_plan), emergency_sku_reroute_status # Return the new list

# Helper function to get aggregated disruption data
def get_disruption_history_data():
    all_disruptions = []
    # Fetch from weather, news, social_media, labor, logistics collections
    collections_to_check = [weather_collection, news_collection, social_media_collection, labor_collection, logistics_collection]
    
    # Get data for the last 30 days
    thirty_days_ago = time.time() - (30 * 24 * 3600)

    for col in collections_to_check:
        docs = list(col.find({"timestamp": {"$gte": thirty_days_ago}}, {"location": 1, "timestamp": 1, "description": 1, "source": 1, "title": 1}))
        for doc in docs:
            # Ensure 'description' or 'title' exists for disruption text
            disruption_text = doc.get('description', doc.get('title', 'No description'))
            all_disruptions.append({
                "date": datetime.fromtimestamp(doc['timestamp']).strftime('%Y-%m-%d'),
                "hour": datetime.fromtimestamp(doc['timestamp']).hour,
                "location": doc.get('location', 'General'),
                "type": doc.get('source', 'Unknown'), # Use 'source' field as type (e.g., 'news', 'social_media_reddit')
                "description": disruption_text
            })
    
    if not all_disruptions:
        return pd.DataFrame(), pd.DataFrame() # Return empty DFs for both

    df_disruptions = pd.DataFrame(all_disruptions)
    df_disruptions['date'] = pd.to_datetime(df_disruptions['date'])
    df_disruptions['count'] = 1 # Helper column for counting
    
    # Aggregate by date for the bar chart
    daily_disruptions = df_disruptions.groupby('date').size().reset_index(name='count')
    daily_disruptions.columns = ['Date', 'Disruption Count']
    
    return daily_disruptions, df_disruptions # Return both aggregated and raw for drill-down


# Cached FC Data Function
def get_fc_data(mode, selected_scenario):
    fcs, fc_to_city, fc_to_fc_id, fc_id_to_name, fc_coordinates = get_fcs()
    if not fcs:
        logger.info("No FCs to process, yielding empty list.")
        return # Generator simply finishes
    
    risk_data = {}
    if mode == "Simulation Mode" and selected_scenario:
        scenario = scenarios[selected_scenario]
        affected_cities = scenario.get("affected_cities", [])
        affected_fcs = scenario.get("affected_fcs", [])
        logger.info(f"Simulation Mode: Scenario={selected_scenario}, Affected Cities={affected_cities}") # Use affected_cities for logging
    else:
        affected_cities = []
        affected_fcs = []
        logger.info("Real Mode: No simulation scenario applied.")
    
    # Store processed rows here for summary analytics (will be collected by the caller)
    # This list is *not* yielded all at once anymore. Individual rows are yielded.
    
    for fc in fcs:
        city = fc_to_city[fc]
        fc_id = fc_to_fc_id[fc]
        if mode == "Simulation Mode" and selected_scenario:
            simulated_weather = get_simulated_weather(scenario, city) if city in affected_cities else None
            simulated_social_media = get_simulated_social_media(scenario, city) if city in affected_cities else None
            simulated_inventory = get_simulated_inventory(scenario, fc, fc_id) if fc in affected_fcs else None
        else:
            simulated_weather = None
            simulated_social_media = None
            simulated_inventory = None
        
        try:
            risk_prompt = generate_risk_prompt(
                fc,
                city,
                simulated_weather=simulated_weather, # Passed simulated_weather here
                simulated_social_media=simulated_social_media, # Passed simulated_social_media here
                simulated_inventory=simulated_inventory
            )
            risk_score, status, reasoning, emergency_classifications_from_gemini = gemini_predict(risk_prompt, fc_name=fc)
            
            # Generate contingency plan, getting summary, full detail, and emergency SKU status
            contingency_plan_summary, contingency_plan_full_detail, emergency_sku_reroute_status = generate_contingency_plan(
                fc, city, risk_score, risk_data, fc_coordinates,
                emergency_classifications_from_gemini,
                shipments_collection, fulfillment_centers_collection # Pass collections
            )
            
            gemini_prompts_collection.insert_one({
                "fc_name": fc,
                "city": city,
                "prompt_text": risk_prompt,
                "timestamp": get_est_datetime(),
                "emergency_classifications": emergency_classifications_from_gemini,
                "reasoning": reasoning,
                "contingency_plan_full": contingency_plan_full_detail, # Store full plan for detail page
                "emergency_sku_reroute_status": emergency_sku_reroute_status # Store for pie chart
            })
            
            risk_data[fc] = {"Risk Score": risk_score, "Status": status, "Reasoning": reasoning}
            
            row = {
                "FC Name": f'<a href="?selected_fc={fc_id}&view=inventory">{fc}</a>', # HTML link for FC Name
                "City": city,
                "Risk Score": risk_score,
                "Status": status,
                "Contingency Plan": contingency_plan_summary, # Show only summary in dashboard table
                "Last Updated (EST)": get_est_datetime(),
                "Reasoning": f'<a href="?selected_fc={fc_id}&view=reasoning">View Reasoning</a>', # HTML link for Reasoning
                "View Plan": f'<a href="?selected_fc={fc_id}&view=contingency_plan">View Plan</a>' # HTML link for View Plan
            }
            yield row # Yield individual row for streaming
        except Exception as e:
            logger.error(f"Error processing FC {fc}: {str(e)}")
            row = {
                "FC Name": fc,
                "City": city,
                "Risk Score": 50,
                "Status": "Unknown",
                "Contingency Plan": "Error processing data",
                "Last Updated (EST)": get_est_datetime(),
                "Reasoning": f"Error: {str(e)}", # Display error message in Reasoning column
                "View Plan": "N/A" # No link on error
            }
            yield row # Yield individual error row


# Streamlit Dashboard
try:
    st.title("Supply Chain FC Risk Dashboard")
    
    if "mode" not in st.session_state:
        st.session_state.mode = "Real Mode"
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = time.time()
    
    mode = st.sidebar.selectbox(
        "Select Mode",
        ["Real Mode", "Simulation Mode"],
        index=["Real Mode", "Simulation Mode"].index(st.session_state.mode)
    )
    st.session_state.mode = mode
    
    if mode == "Simulation Mode":
        if "selected_scenario" not in st.session_state:
            st.session_state.selected_scenario = list(scenarios.keys())[0]
        selected_scenario = st.sidebar.selectbox(
            "Select Scenario",
            list(scenarios.keys()),
            index=list(scenarios.keys()).index(st.session_state.selected_scenario)
        )
        st.session_state.selected_scenario = selected_scenario
        st.sidebar.write(f"Simulation Mode Active: {selected_scenario}")
    else:
        selected_scenario = None
    
    if st.session_state.mode == "Real Mode":
        current_time = time.time()
        if current_time - st.session_state.last_refresh > 300:
            logger.info("Automatic refresh triggered")
            st.session_state.last_refresh = time.time()
            st.rerun()
    
    st.info("Real-time FC risk statuses and contingency plans. In Real Mode, data refreshes every 5 minutes automatically.")
    
    if st.button("Refresh Data Now"):
        logger.info("Manual refresh triggered")
        st.session_state.last_refresh = time.time()
        st.rerun()
    
    fcs, fc_to_city, fc_to_fc_id, fc_id_to_name, fc_coordinates = get_fcs()
    if not fcs:
        st.stop()
    
    selected_fc = st.query_params.get("selected_fc", None)
    selected_view = st.query_params.get("view", "dashboard")
    
    if selected_fc and selected_view == "inventory":
        fc_name = fc_id_to_name.get(selected_fc, "Unknown FC")
        st.write(f"Inventory for {fc_name} ({selected_fc})")
        inventory_docs = list(inventory_collection.find({"FC_ID": selected_fc}))
        if inventory_docs:
            # Prepare data for Inventory Emergency Products Pie Chart
            emergency_inventory_count = 0
            non_emergency_inventory_count = 0
            
            # Collect SKUs with their quantities and emergency status from the current FC's inventory_docs
            inventory_sku_details = []
            for doc in inventory_docs:
                sku = doc.get("Product_SKU")
                quantity = doc.get("Quantity", 0)
                is_emergency = doc.get("Is_Emergency_Defined", False) # Check for Is_Emergency_Defined from dynamic_data_generation
                
                inventory_sku_details.append({
                    "SKU": sku,
                    "Quantity": quantity,
                    "Is_Emergency": is_emergency,
                    "Display_Text": f"{sku} ({quantity})"
                })
                if is_emergency:
                    emergency_inventory_count += quantity
                else:
                    non_emergency_inventory_count += quantity

            st.dataframe(pd.DataFrame(inventory_docs), use_container_width=True) # Original table

            if emergency_inventory_count > 0 or non_emergency_inventory_count > 0:
                chart_data_inv = pd.DataFrame({
                    'Category': ['Emergency Products', 'Non-Emergency Products'],
                    'Quantity': [emergency_inventory_count, non_emergency_inventory_count],
                    'SKUs_Hover': [
                        '<br>'.join([s['Display_Text'] for s in inventory_sku_details if s['Is_Emergency']][:5]) + ('...' if len([s for s in inventory_sku_details if s['Is_Emergency']]) > 5 else ''),
                        '<br>'.join([s['Display_Text'] for s in inventory_sku_details if not s['Is_Emergency']][:5]) + ('...' if len([s for s in inventory_sku_details if not s['Is_Emergency']]) > 5 else '')
                    ]
                })

                st.subheader(f"Inventory Product Distribution for {fc_name}")
                fig_inv = px.pie(
                    chart_data_inv,
                    values='Quantity',
                    names='Category',
                    title='Emergency vs. Non-Emergency Products in Inventory',
                    color='Category',
                    color_discrete_map={'Emergency Products': 'red', 'Non-Emergency Products': 'green'}
                )
                fig_inv.update_traces(
                    hovertemplate='<b>%{label}</b><br>Quantity: %{value}<br>SKUs: %{customdata}<extra></extra>',
                    customdata=chart_data_inv['SKUs_Hover']
                )
                st.plotly_chart(fig_inv, use_container_width=True, key="inventory_pie") # Unique key
            else:
                st.info("No inventory data to display distribution chart.")

        else:
            st.write("No inventory found")
        if st.button("Back to Dashboard"):
            st.query_params["view"] = "dashboard"
            st.rerun()
    elif selected_fc and selected_view == "reasoning":
        fc_name = fc_id_to_name.get(selected_fc, "Unknown FC")
        st.write(f"Reasoning for {fc_name} ({selected_fc})")
        prompt_doc = gemini_prompts_collection.find_one({"fc_name": fc_name}, sort=[("timestamp", -1)])
        if prompt_doc:
            with st.expander("View Input Data"):
                st.markdown(prompt_doc["prompt_text"])
            st.subheader("Reasoning")
            st.markdown(prompt_doc["reasoning"])
        else:
            st.write(f"No reasoning found for {fc_name} ({selected_fc})")
        if st.button("Back to Dashboard"):
            st.query_params["view"] = "dashboard"
            st.rerun()
    # --- Contingency Plan Detail Page ---
    elif selected_fc and selected_view == "contingency_plan":
        fc_name = fc_id_to_name.get(selected_fc, "Unknown FC")
        st.write(f"Contingency Plan for {fc_name} ({selected_fc})")
        prompt_doc = gemini_prompts_collection.find_one({"fc_name": fc_name}, sort=[("timestamp", -1)])
        if prompt_doc and "contingency_plan_full" in prompt_doc:
            with st.expander("View Full Context (Input Data & Reasoning)"):
                st.markdown("**Input Data:**\n" + prompt_doc["prompt_text"])
                st.markdown("\n**Reasoning:**\n" + prompt_doc["reasoning"])
            st.subheader("Detailed Contingency Plan")
            st.markdown(prompt_doc["contingency_plan_full"])

            # --- NEW: Emergency Products Re-routing Status Pie Chart ---
            if "emergency_sku_reroute_status" in prompt_doc and prompt_doc["emergency_sku_reroute_status"]:
                df_reroute_status = pd.DataFrame(prompt_doc["emergency_sku_reroute_status"])
                
                # Group by status and count SKUs
                status_counts = df_reroute_status.groupby('Status').size().reset_index(name='Count')
                
                # Group by status and list SKUs for hover text
                sku_lists_by_status = df_reroute_status.groupby('Status')['SKU'].apply(lambda x: ', '.join(x)).reset_index(name='SKUs_List')

                chart_data_reroute = status_counts.merge(sku_lists_by_status, on='Status')

                st.subheader("Emergency SKU Re-routing Status")
                fig_reroute = px.pie(
                    chart_data_reroute,
                    values='Count',
                    names='Status',
                    title='Re-routing Status of Identified Emergency SKUs',
                    color='Status',
                    color_discrete_map={
                        'Re-routed': 'green',
                        'No Active Shipment': 'grey',
                        'No Optimal Route': 'red',
                        'Missing Destination Coords': 'orange'
                    }
                )
                fig_reroute.update_traces(
                    hovertemplate='<b>%{label}</b><br>Count: %{value}<br>SKUs: %{customdata[0]}<extra></extra>',
                    customdata=chart_data_reroute[['SKUs_List']].values # Pass SKUs for hover text
                )
                st.plotly_chart(fig_reroute, use_container_width=True, key="contingency_pie") # Unique key
            else:
                st.info("No emergency SKUs were identified or their re-routing status could not be determined for a chart.")
            # --- END NEW: Emergency Products Re-routing Status Pie Chart ---

        else:
            st.write(f"No detailed contingency plan found for {fc_name} ({selected_fc})")
        if st.button("Back to Dashboard"):
            st.query_params["view"] = "dashboard"
            st.rerun()
    # --- Main Dashboard View ---
    else:
        cache_key = f"{st.session_state.mode}_{selected_scenario}_{st.session_state.last_refresh}"
        
        # --- NEW: Placeholders for dynamic updates ---
        # These placeholders will be updated in each iteration of the loop
        summary_analytics_placeholder = st.empty()
        risk_pie_chart_placeholder = st.empty() # Placeholder for the pie chart
        disruption_bar_chart_placeholder = st.empty() # Placeholder for the bar chart
        table_display_placeholder = st.empty()
        # --- END NEW: Placeholders ---

        if "fc_data_cache" not in st.session_state or st.session_state.get("fc_data_cache_key") != cache_key:
            st.session_state.fc_data_cache = [] # Initialize empty cache
            st.session_state.fc_data_cache_key = cache_key
            
            with st.spinner("Fetching data for Fulfillment Centers..."):
                # Iterate through the generator, which yields one row at a time
                for row_data in get_fc_data(st.session_state.mode, selected_scenario):
                    # In get_fc_data, if no FCs are found, it just returns. Need to handle that.
                    if isinstance(row_data, list) and not row_data: # If get_fcs returns empty list (edge case)
                        break # Exit loop if no FCs to process
                    
                    st.session_state.fc_data_cache.append(row_data) # Add row to cache
                    
                    df_display = pd.DataFrame(st.session_state.fc_data_cache)
                    
                    # --- Render Summary Analytics (Updates dynamically) ---
                    total_fcs = len(df_display)
                    high_risk_fcs = len(df_display[df_display['Risk Score'] > 70])
                    medium_risk_fcs = len(df_display[(df_display['Risk Score'] > 30) & (df_display['Risk Score'] <= 70)])
                    contingency_options_fcs = len(df_display[
                        df_display['Contingency Plan'].str.contains("Re-routing options available|Re-routing evaluation needed", na=False)
                    ])
                    
                    with summary_analytics_placeholder.container(): # Use container to clear and re-render
                        st.subheader("FC Network Overview")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1: st.metric("Total FCs", total_fcs)
                        with col2: st.metric("High Risk FCs", high_risk_fcs)
                        with col3: st.metric("Medium Risk FCs", medium_risk_fcs)
                        with col4: st.metric("FCs with Plans", contingency_options_fcs)
                        st.markdown("---") # Separator
                    # --- End Render Summary Analytics ---

                    # --- NEW: FC Risk Distribution Pie Chart (Updates dynamically) ---
                    if not df_display.empty:
                        df_risk_counts = df_display.groupby('Status').size().reset_index(name='Count')
                        df_risk_counts['FCs'] = df_risk_counts['Status'].apply(lambda x: ', '.join(df_display[df_display['Status'] == x]['FC Name'].str.replace(r'<[^>]*>', '', regex=True).tolist())) # Get FC names for hover
                        
                        fig_risk_pie = px.pie(
                            df_risk_counts,
                            values='Count',
                            names='Status',
                            title='FC Risk Distribution',
                            color='Status', # Color slices by Status
                            color_discrete_map={ # Define colors for consistency
                                'High Risk': 'red', 'Medium Risk': 'orange', 'Low Risk': 'green', 'Unknown': 'grey',
                                'Error processing data': 'darkred',
                                'Re-routing evaluation needed (No Emergency SKUs)': 'lightblue',
                                'Re-routing evaluation needed (Risk:': 'orange',
                                'Re-routing evaluation needed (Emergency SKUs, Risk:': 'darkorange',
                                'Re-routing options available': 'blue',
                                'Re-routing evaluation: No optimal routes found': 'purple',
                                'No active emergency shipments found': 'grey'
                            }
                        )
                        fig_risk_pie.update_traces(
                            hovertemplate='<b>%{label}</b><br>Count: %{value}<br>FCs: %{customdata}<extra></extra>',
                            customdata=df_risk_counts['FCs'] # Pass FC names for hover
                        )
                        with risk_pie_chart_placeholder.container():
                            st.subheader("FC Risk Distribution")
                            st.plotly_chart(fig_risk_pie, use_container_width=True, key=f"risk_pie_chart_dynamic_{len(df_display)}") # Unique key based on data length
                            st.markdown("---")
                    # --- END NEW: FC Risk Distribution Pie Chart ---

                    # --- NEW: Disruption History Bar Chart (Updates dynamically) ---
                    daily_disruptions_df, raw_disruptions_df = get_disruption_history_data() # Fetch data for chart
                    if not daily_disruptions_df.empty:
                        fig_disruption_bar = px.bar(
                            daily_disruptions_df,
                            x='Date',
                            y='Disruption Count',
                            title='Recent Disruption Events by Date',
                            hover_data={'Date': '|%Y-%m-%d', 'Disruption Count': True}, # Customize hover for date
                            color_discrete_sequence=px.colors.qualitative.Plotly # Use a nice color scheme
                        )
                        fig_disruption_bar.update_traces(marker_color='lightblue') # Make bars light blue
                        with disruption_bar_chart_placeholder.container():
                            st.subheader("Recent Disruption History")
                            st.plotly_chart(fig_disruption_bar, use_container_width=True, key=f"disruption_bar_chart_dynamic_{len(df_display)}") # Unique key based on data length
                            st.markdown("---")
                    # --- END NEW: Disruption History Bar Chart ---

                    # --- Main Dashboard Table Display (Updates dynamically) ---
                    table_display_placeholder.markdown(df_display.to_html(escape=False), unsafe_allow_html=True)
                    # --- End Main Dashboard Table Display ---
                    
                    # Optional: Add a small sleep to visually see updates for very fast connections
                    # time.sleep(0.1) 
        else: # Display cached data (once all data is loaded or on rerun)
            df_display = pd.DataFrame(st.session_state.fc_data_cache)

            # --- Display cached Summary Analytics ---
            total_fcs = len(df_display)
            high_risk_fcs = len(df_display[df_display['Risk Score'] > 70])
            medium_risk_fcs = len(df_display[(df_display['Risk Score'] > 30) & (df_display['Risk Score'] <= 70)])
            contingency_options_fcs = len(df_display[
                df_display['Contingency Plan'].str.contains("Re-routing options available|Re-routing evaluation needed", na=False)
            ])

            st.subheader("FC Network Overview")
            col1, col2, col3, col4 = st.columns(4)
            with col1: st.metric("Total FCs", total_fcs)
            with col2: st.metric("High Risk FCs", high_risk_fcs)
            with col3: st.metric("Medium Risk FCs", medium_risk_fcs)
            with col4: st.metric("FCs with Plans", contingency_options_fcs)
            st.markdown("---") # Separator
            # --- End Display cached Summary Analytics ---

            # --- Display cached FC Risk Distribution Pie Chart ---
            if not df_display.empty:
                df_risk_counts = df_display.groupby('Status').size().reset_index(name='Count')
                df_risk_counts['FCs'] = df_display['Status'].apply(lambda x: ', '.join(df_display[df_display['Status'] == x]['FC Name'].str.replace(r'<[^>]*>', '', regex=True).tolist()))
                
                fig_risk_pie = px.pie(
                    df_risk_counts,
                    values='Count',
                    names='Status',
                    title='FC Risk Distribution',
                    color='Status',
                    color_discrete_map={
                        'High Risk': 'red', 'Medium Risk': 'orange', 'Low Risk': 'green', 'Unknown': 'grey',
                        'Error processing data': 'darkred',
                        'Re-routing evaluation needed (No Emergency SKUs)': 'lightblue',
                        'Re-routing evaluation needed (Risk:': 'orange',
                        'Re-routing evaluation needed (Emergency SKUs, Risk:': 'darkorange',
                        'Re-routing options available': 'blue',
                        'Re-routing evaluation: No optimal routes found': 'purple',
                        'No active emergency shipments found': 'grey'
                    }
                )
                fig_risk_pie.update_traces(
                    hovertemplate='<b>%{label}</b><br>Count: %{value}<br>FCs: %{customdata}<extra></extra>',
                    customdata=df_risk_counts['FCs']
                )
                st.subheader("FC Risk Distribution")
                st.plotly_chart(fig_risk_pie, use_container_width=True, key="risk_pie_chart_cached") # Unique key for cached
                st.markdown("---")
            # --- End Display cached FC Risk Distribution Pie Chart ---

            # --- Display cached Disruption History Bar Chart ---
            daily_disruptions_df, raw_disruptions_df = get_disruption_history_data()
            if not daily_disruptions_df.empty:
                fig_disruption_bar = px.bar(
                    daily_disruptions_df,
                    x='Date',
                    y='Disruption Count',
                    title='Recent Disruption Events by Date',
                    hover_data={'Date': '|%Y-%m-%d', 'Disruption Count': True},
                    color_discrete_sequence=px.colors.qualitative.Plotly
                )
                fig_disruption_bar.update_traces(marker_color='lightblue')
                st.subheader("Recent Disruption History")
                st.plotly_chart(fig_disruption_bar, use_container_width=True, key="disruption_bar_chart_cached") # Unique key for cached
                st.markdown("---")
            # --- End Display cached Disruption History Bar Chart ---

            # --- Main Dashboard Table Display (for cached data) ---
            table_display_placeholder.markdown(df_display.to_html(escape=False), unsafe_allow_html=True)
            # --- End Main Dashboard Table Display ---
            
        if not st.session_state.fc_data_cache:
            st.warning("No data available to display. Please check the logs or ensure MongoDB is populated.")

except Exception as e:
    logger.error(f"Unexpected error: {str(e)}")
    st.error(f"An unexpected error occurred: {str(e)}. Please check the logs for details.")