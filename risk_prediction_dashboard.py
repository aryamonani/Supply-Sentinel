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
import plotly.express as px

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Custom CSS for Purity UI Dashboard Inspiration ---
st.markdown("""
<style>
/* Overall app background - Cream Gradient */
.stApp {
    background: linear-gradient(to right, #FFFACD, #FDF5E6);
    color: black;
}

/* Sidebar styling for frosted glass effect */
[data-testid="stSidebar"] {
    background: rgba(255, 255, 255, 0.2);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    border-right: 1px solid rgba(255, 255, 255, 0.1);
}
[data-testid="stSidebar"] * {
    color: black;
}
[data-testid="stSidebar"] p, [data-testid="stSidebar"] label {
    color: black !important;
}

/* --- REVISED AND MORE SPECIFIC RULES FOR SELECTBOX --- */
[data-testid="stSidebar"] [data-testid="stSelectbox"] {
    background-color: white !important;
    border-radius: 8px;
    padding: 2px;
}
[data-testid="stSidebar"] [data-testid="stSelectbox"] div[data-baseweb="select"] {
    background-color: white !important;
    border: 1px solid #ccc !important;
    border-radius: 5px !important;
}
[data-testid="stSidebar"] [data-testid="stSelectbox"] div[data-baseweb="select"] > div {
    background-color: transparent !important;
    color: black !important;
}
[data-testid="stSidebar"] [data-testid="stSelectbox"] svg {
    fill: black !important;
}
div[data-baseweb="popover"] ul[role="listbox"] {
    background-color: white !important;
    border: 1px solid #ccc !important;
    border-radius: 5px !important;
}
div[data-baseweb="popover"] ul[role="listbox"] li {
    color: black !important;
    background-color: white !important;
}
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
load_dotenv()
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

# Fetch FCs dynamically from database with coordinates
def get_fcs():
    try:
        fc_docs = list(fulfillment_centers_collection.find({}, {"FC_Name": 1, "FC_ID": 1, "city": 1, "Latitude": 1, "Longitude": 1, "re_routing_cost_multiplier": 1, "re_routing_tat_adder_days": 1}))
        if not fc_docs:
            logger.warning("No FCs found in the database.")
            st.warning("No Fulfillment Centers found in the database. Please ensure data is populated in MongoDB.")
            return [], {}, {}, {}, {}
        fc_to_city = {doc["FC_Name"]: doc["city"] for doc in fc_docs}
        fc_to_fc_id = {doc["FC_Name"]: doc["FC_ID"] for doc in fc_docs}
        fc_id_to_name = {doc["FC_ID"]: doc["FC_Name"] for doc in fc_docs}
        fc_coordinates = {
            doc["FC_ID"]: {
                "coords": (doc["Latitude"], doc["Longitude"]),
                "cost_multiplier": doc.get("re_routing_cost_multiplier", 1.2),
                "tat_adder": doc.get("re_routing_tat_adder_days", 1)
            } for doc in fc_docs
        }
        logger.info(f"Fetched {len(fc_docs)} FCs from the database: {fc_to_city.keys()}")
        return list(fc_to_city.keys()), fc_to_city, fc_to_fc_id, fc_id_to_name, fc_coordinates
    except Exception as e:
        logger.error(f"Error fetching FCs: {str(e)}")
        st.error(f"Error fetching FCs: {str(e)}")
        return [], {}, {}, {}, {}

# Fetch FC data early to define fc_id_to_name
fcs, fc_to_city, fc_to_fc_id, fc_id_to_name, fc_coordinates = get_fcs()
if not fcs:
    st.stop()

# Retrieve query parameters
selected_fc = st.query_params.get("selected_fc", None)
selected_view = st.query_params.get("view", "dashboard")

# Now use them in the if statement
if selected_fc and selected_view == "contingency_plan":
    fc_name = fc_id_to_name.get(selected_fc, "Unknown FC")
    st.write(f"Contingency Plan for {fc_name} ({selected_fc})")
    prompt_doc = gemini_prompts_collection.find_one({"fc_name": fc_name}, sort=[("timestamp", -1)])
    if prompt_doc and "contingency_plan_full" in prompt_doc:
        contingency_data = prompt_doc["contingency_plan_full"]
        
        # Check if contingency_data is a string (old format) or list of dicts (new format)
        if isinstance(contingency_data, str):
            # Handle old string format (e.g., display as text)
            st.markdown(contingency_data)
        else:
            # Filter and display shipments with re-routing details
            rerouting_shipments = [d for d in contingency_data if d.get('Type') == 'Shipment' and 'Re-routing Destination' in d]
            if rerouting_shipments:
                df_rerouting = pd.DataFrame(rerouting_shipments)
                st.subheader("Shipments with Re-routing Options")
                st.dataframe(df_rerouting[['Shipment ID', 'SKU', 'Re-routing Destination', 'Inventory %', 'Original Cost', 'New Cost', 'Cost Δ', 'Original TAT', 'New TAT', 'TAT Δ']])
            
            # Filter and display SKUs with no active shipments
            no_shipment_skus = [d for d in contingency_data if d.get('Type') == 'No Shipment']
            if no_shipment_skus:
                df_no_shipment = pd.DataFrame(no_shipment_skus)
                st.subheader("SKUs with No Active Shipments")
                st.dataframe(df_no_shipment[['SKU', 'Status']])
            
            # Display additional information messages
            info_messages = [d['Message'] for d in contingency_data if d.get('Type') == 'Info']
            if info_messages:
                st.subheader("Additional Information")
                for msg in info_messages:
                    st.write(msg)

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

# Helper function to get EST datetime
def get_est_datetime():
    est = pytz.timezone("America/New_York")
    return datetime.now(est).isoformat()

# Nearby Cities for Re-routing
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
                           "Silver Spring", "Frederick"],
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
                           "Silver Spring", "Frederick"],
        "event_type": "labor",
        "severity": "extreme"
    },
    "Nearest FC Lacks Inventory (Specific)": {
        "description": "Local disruption, triggering a need for emergency SKUs. The nearest FC to the shipment's destination has only 50% of the required inventory.",
        "affected_cities": ["Houston"],
        "event_type": "inventory",
        "severity": "high",
        "affected_fcs": ["Houston FC 1"]
    },
    "No Nearby FCs Have Sufficient Inventory (Specific)": {
        "description": "Major disruption, no FC within 150 miles has >=90% required inventory for emergency SKUs.",
        "affected_cities": ["Los Angeles"],
        "event_type": "inventory",
        "severity": "moderate",
        "affected_fcs": ["Los Angeles FC X"]
    },
    "Nearest FC Has Partial Inventory (Specific)": {
        "description": "Minor disruption, nearest FC has 80% required inventory for emergency SKUs (below 90% threshold).",
        "affected_cities": ["Miami"],
        "event_type": "inventory",
        "severity": "low",
        "affected_fcs": ["Miami FC X"]
    },
    "Nearest FC Is Outside 150-Mile Radius (Specific)": {
        "description": "Disruption isolates area; nearest FC with sufficient inventory is >150 miles away.",
        "affected_cities": ["Denver"],
        "event_type": "weather",
        "severity": "high",
        "affected_fcs": ["Denver FC X"]
    },
    "Multiple FCs Varying Inventory Levels (Specific)": {
        "description": "Disruption affects inventory; nearest FC has 85% inventory, next has 95%, farther has 100%.",
        "affected_cities": ["Chicago"],
        "event_type": "inventory",
        "severity": "high",
        "affected_fcs": ["Chicago FC X"]
    }
}

# Simulated Data Functions
def get_simulated_weather(scenario, city):
    if scenario["event_type"] == "weather" and city in scenario.get("affected_cities", []):
        return [
            {
                "est_datetime": get_est_datetime(),
                "weather": scenario["description"].lower(),
                "temp": 25 if "hurricane" in scenario["description"].lower() else 20,
                "description": f"{scenario['severity'].capitalize()} severity {scenario['event_type']}"
            }
        ] * 5
    return None

def get_simulated_social_media(scenario, city):
    if scenario["event_type"] in ["weather", "labor"] and city in scenario.get("affected_cities", []):
        return [
            {
                "subreddit": "simulation",
                "created_utc": get_est_datetime(),
                "text": f"Simulated {scenario['event_type']} event in {city}: {scenario['description']}",
                "sentiment": "negative" if scenario["severity"] == "high" else "neutral"
            }
        ] * 5
    return None

def get_simulated_inventory(scenario, fc, fc_id):
    if scenario["event_type"] == "inventory" and fc in scenario.get("affected_fcs", []):
        inventory_docs = list(inventory_collection.find({"FC_ID": fc_id}))
        if "50% of inventory is damaged" in scenario["description"].lower():
            for doc in inventory_docs:
                doc["Quantity"] = doc.get("Quantity", 1) * 0.5
        elif "20% workforce unavailable" in scenario["description"].lower():
            pass
        elif "30% of critical inventory unavailable" in scenario["description"].lower():
            for doc in inventory_docs:
                if doc.get("L1_Category") in ["Health & Household", "Industrial & Scientific"]:
                    doc["Quantity"] = doc.get("Quantity", 1) * 0.7
        elif "refrigeration failure spoils 25% of perishable goods" in scenario["description"].lower():
            for doc in inventory_docs:
                if doc.get("L1_Category") == "Grocery & Gourmet Food":
                    doc["Quantity"] = doc.get("Quantity", 1) * 0.75
        elif "40% excess inventory" in scenario["description"].lower():
            for doc in inventory_docs:
                doc["Quantity"] = doc.get("Quantity", 1) * 1.4
        elif "20% of incoming inventory delayed" in scenario["description"].lower():
            pass
        elif "nearest fc lacks inventory" in scenario["description"].lower():
            for doc in inventory_docs:
                if doc.get("Is_Emergency_Defined"):
                    doc["Quantity"] = doc.get("Quantity", 1) * 0.5
        elif "no nearby fcs have sufficient inventory" in scenario["description"].lower():
            for doc in inventory_docs:
                if doc.get("Is_Emergency_Defined"):
                    doc["Quantity"] = doc.get("Quantity", 1) * 0.3
        elif "nearest fc has partial inventory" in scenario["description"].lower():
            for doc in inventory_docs:
                if doc.get("Is_Emergency_Defined"):
                    doc["Quantity"] = doc.get("Quantity", 1) * 0.8
        elif "multiple fcs varying inventory levels" in scenario["description"].lower():
            pass
        return inventory_docs
    return None

def get_simulated_labor(scenario, city):
    if scenario["event_type"] == "labor" and city in scenario.get("affected_cities", []):
        return [
            {
                "timestamp": get_est_datetime(),
                "description": f"Simulated labor disruption: {scenario['description']}",
                "severity": scenario["severity"],
                "source": "Simulation"
            }
        ] * 5
    return None

def get_simulated_news(scenario, city):
    if scenario["event_type"] in ["other", "labor"] and city in scenario.get("affected_cities", []):
        return [
            {
                "timestamp": get_est_datetime(),
                "description": f"Simulated news: {scenario['description']}",
                "impact": scenario["severity"],
                "source": "Simulation"
            }
        ] * 5
    return None

def get_simulated_logistics(scenario, city):
    if scenario["event_type"] == "logistics" and city in scenario.get("affected_cities", []):
        return [
            {
                "est_datetime": get_est_datetime(),
                "description": f"Simulated logistics issue: {scenario['description']}",
                "disruption_level": scenario["severity"],
                "source": "Simulation"
            }
        ] * 5
    return None

# Gemini Prediction Function
import re
import logging

logger = logging.getLogger(__name__)

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
            "gemini-1.5-flash-latest", "models/gemini-1.5-flash-latest",
            "gemini-2.0-flash", "models/gemini-2.0-flash",
            "gemini-1.5-flash-002", "models/gemini-1.5-flash-002",
            "gemini-1.5-flash", "models/gemini-1.5-flash",
            "gemini-2.5-pro-preview-06-05", "models/gemini-2.5-pro-preview-06-05",  
            "gemini-1.5-pro-latest", "models/gemini-1.5-pro-latest",
            "gemini-1.5-pro-002", "models/gemini-1.5-pro-002",
            "gemini-1.5-pro", "models/gemini-1.5-pro",
            "gemini-1.0-pro-latest", "models/gemini-1.0-pro-latest",
            "gemini-1.0-pro-001", "models/gemini-1.0-pro-001",
            "gemini-1.0-pro", "models/gemini-1.0-pro",
            "gemini-pro", "models/gemini-pro",
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
      
        # Default values
        risk_score = 50
        status = "Unknown"
        reasoning = ""
        emergency_classifications = []
      
        # Extract Risk Score
        risk_score_match = re.search(r"Risk Score:\s*(\d+\.?\d*)", generated_text)
        if risk_score_match:
            risk_score = float(risk_score_match.group(1))
          
        # Extract Status
        status_match = re.search(r"Status:\s*(.+)", generated_text)
        if status_match:
            raw_status = status_match.group(1).strip()
            cleaned_status = re.sub(r'\*+', '', raw_status).strip()
            status = cleaned_status.title()
            if status not in ["Low Risk", "Medium Risk", "High Risk"]:
                status = "Low Risk"
                logger.warning(f"Unexpected status format for FC {fc_name}: {raw_status}, defaulting to 'Low Risk'")
              
        # Extract Reasoning
        reasoning_section_match = re.search(r"Reasoning:\s*(.*?)(?=\nEmergency Classifications:|\Z)", generated_text, re.DOTALL)
        if reasoning_section_match:
            reasoning = reasoning_section_match.group(1).strip()
        else:
            reasoning = "No reasoning provided."
          
        # Extract Emergency Classifications
        start_index = generated_text.find("Emergency Classifications:")
        if start_index != -1:
            emergency_text = generated_text[start_index + len("Emergency Classifications:"):].strip()
            lines = emergency_text.split('\n')
            for line in lines:
                line = line.strip()
                if line:
                    parts = [part.strip() for part in line.split(',')]
                    if len(parts) >= 3:
                        sku_part = parts[0]
                        emergency_part = parts[1]
                        reason_part = ', '.join(parts[2:])
                      
                        sku = sku_part.split(':')[1].strip() if ':' in sku_part else sku_part
                        emergency = emergency_part.split(':')[1].strip() == "True" if ':' in emergency_part else emergency_part == "True"
                        reason = reason_part.split(':')[1].strip() if ':' in reason_part else reason_part
                      
                        emergency_classifications.append({"SKU": sku, "Emergency": emergency, "Reason": reason})
        else:
            logger.warning(f"No Emergency Classifications found for FC {fc_name}")
          
        logger.info("Successfully received response from Gemini API")
        return risk_score, status, reasoning, emergency_classifications
  
    except Exception as e:
        logger.error(f"Gemini prediction error with model {model_name}: {str(e)}")
        return 50, "Low Risk", f"Gemini prediction error: {str(e)}", []
    
# Generate Risk Prompt for Gemini
def generate_risk_prompt(fc_name, city, fc_id, event_type, simulated_weather, simulated_social_media, simulated_inventory, simulated_news, simulated_labor, simulated_logistics):
    current_time = time.time()
    time_threshold = current_time - 86400
  
    prompt = f"""
    You are an AI expert in supply chain risk management for Amazon Fulfillment Centers (FCs). Your task is to:
    1. Assess the risk of disruption for the {fc_name} located in {city} based on the provided data.
    2. Determine if products in the FC's inventory belong to emergency categories critical for public health and safety.
    3. Provide a risk score (0-100) and classify the risk status strictly as one of: "Low Risk", "Medium Risk", or "High Risk".

    **COMPULSORY**: 
    - Do NOT use asterisks (**) or any markdown formatting in the status field.
    - Do NOT use "Unknown" or any status other than "Low Risk", "Medium Risk", or "High Risk".
    - Every FC must have a risk status assigned.
    - Show your reasoning and emergency category classifications.
    - If using simulation data, treat it as real data and proceed normally.

    ### Data for {fc_name} ({city})
    #### Weather Data (Last 24 Hours)
    """
    weather_data = simulated_weather if event_type == "weather" and simulated_weather is not None else list(
        weather_collection.find({"location": city, "timestamp": {"$gte": time_threshold}}).sort("timestamp", -1)
    )
    if not weather_data:
        prompt += "No recent weather data available.\n"
    else:
        for doc in weather_data:
            prompt += f"- {doc.get('est_datetime', 'N/A')}: Weather {doc.get('weather', 'N/A')}, Temp {doc.get('temp', 'N/A')}°C, Conditions: {doc.get('description', 'N/A')}\n"
          
    prompt += """
    #### Social Media (Reddit, Last 24 Hours)
    """
    social_data = simulated_social_media if event_type in ["weather", "labor"] and simulated_social_media is not None else list(
        social_media_collection.find({"location": city, "timestamp": {"$gte": time_threshold}}).sort("timestamp", -1)
    )
    if not social_data:
        prompt += "No recent social media data available.\n"
    else:
        for doc in social_data:
            post_text = doc.get('text', 'N/A')
            subreddit = doc.get('subreddit', 'N/A')
            created_utc = doc.get('created_utc', 'N/A')
            sentiment = doc.get('sentiment', 'Neutral')
            prompt += f"- r/{subreddit} ({created_utc}): \"{post_text}\" (Sentiment: {sentiment})\n"
          
    prompt += """
    #### News (Last 24 Hours)
    """
    news_data = simulated_news if event_type in ["other", "labor"] and simulated_news is not None else list(
        news_collection.find({"location": city, "timestamp": {"$gte": time_threshold}}).sort("timestamp", -1)
    )
    if not news_data:
        prompt += "No recent news data available.\n"
    else:
        for doc in news_data:
            prompt += f"- Reuters ({doc.get('timestamp', 'N/A')}): \"{doc.get('description', 'N/A')}\" (Impact: {doc.get('impact', 'Unknown')})\n"
          
    prompt += """
    #### Labor (Last 24 Hours)
    """
    labor_data = simulated_labor if event_type == "labor" and simulated_labor is not None else list(
        labor_collection.find({"location": city, "timestamp": {"$gte": time_threshold}}).sort("timestamp", -1)
    )
    if not labor_data:
        prompt += "No recent labor data available.\n"
    else:
        for doc in labor_data:
            prompt += f"- Reuters ({doc.get('timestamp', 'N/A')}): \"{doc.get('description', 'N/A')}\" (Severity: {doc.get('severity', 'Unknown')})\n"
          
    prompt += """
    #### Logistics (Last 24 Hours)
    """
    logistics_data = simulated_logistics if event_type == "logistics" and simulated_logistics is not None else list(
        logistics_collection.find({"location": city, "timestamp": {"$gte": time_threshold}}).sort("timestamp", -1)
    )
    if not logistics_data:
        prompt += "No recent logistics data available.\n"
    else:
        for doc in logistics_data:
            prompt += f"- FreightWaves ({doc.get('est_datetime', 'N/A')}): \"{doc.get('description', 'N/A')}\" (Disruption Level: {doc.get('disruption_level', 'Unknown')})\n"
          
    prompt += """
    #### Inventory (All Products)
    """
    inventory_data = simulated_inventory if event_type == "inventory" and simulated_inventory is not None else list(
        inventory_collection.find({"FC_ID": fc_id})
    )
    if not inventory_data:
        prompt += "No inventory data available.\n"
    else:
        for doc in inventory_data:
            prompt += f"- SKU: {doc.get('Product_SKU', 'N/A')}, Category: {doc.get('L1_Category', 'N/A')}, Description: {doc.get('Product_Description', 'N/A')}, Quantity: {doc.get('Quantity', 'N/A')}\n"
          
    prompt += """
    ### Instructions
    1. **Risk Assessment**:
        - Analyze the data to predict the risk of disruption (0-100).
        - Explain your reasoning for the risk score.
        - Classify the risk status as exactly one of: "Low Risk", "Medium Risk", or "High Risk".
          - Do NOT use asterisks (**) or any markdown in the status.
          - Do NOT use "Unknown" or any other status.
          - Assign a status even if data is limited (default to "Low Risk" if no risk factors are present).

    2. **Emergency Category Classification**:
        - For each product, determine if it’s an emergency item based on category and description (e.g., health, safety items).
        - Output SKUs with emergency status (True/False) and reasoning.

    3. **Output Format**:
        - Risk Score: [number]
        - Status: [Low Risk | Medium Risk | High Risk]
        - Reasoning: [text]
        - Emergency Classifications:
            - SKU: [sku], Emergency: [True/False], Reason: [text]

    **Example Output**:
    - Risk Score: 75
    - Status: High Risk
    - Reasoning: Severe weather conditions indicate a high likelihood of disruption.
    - Emergency Classifications:
        - SKU: ABC123, Emergency: True, Reason: Health-related product critical during disruptions.
        - SKU: XYZ789, Emergency: False, Reason: Non-critical electronics item.
    """
    return prompt

# Updated Contingency Plan Logic
def generate_contingency_plan(fc_name, city, risk_score, risk_data, fc_coordinates, current_emergency_classifications, shipments_collection, fulfillment_centers_collection):
    full_contingency_plan = []
    emergency_skus = [c["SKU"] for c in current_emergency_classifications if c["Emergency"]]
    summary_status = "No re-routing needed"
    should_evaluate_rerouting = False
    gemini_status = risk_data.get(fc_name, {}).get("Status", "Low Risk")  # Default to Low Risk if missing
  
    if gemini_status == "High Risk":
        should_evaluate_rerouting = True
        full_contingency_plan.append({"Type": "Info", "Message": "Gemini assessed status as High Risk. Immediate re-routing evaluation triggered."})
    elif gemini_status == "Medium Risk":
        should_evaluate_rerouting = True
        full_contingency_plan.append({"Type": "Info", "Message": "Gemini assessed status as Medium Risk. Re-routing evaluation for potential disruptions triggered."})
    elif emergency_skus and any(word in risk_data.get(fc_name, {}).get("Reasoning", "")for word in ["disruption", "delay", "impact", "traffic", "issue", "risk"]):
        should_evaluate_rerouting = True
        full_contingency_plan.append({"Type": "Info", "Message": "Emergency SKUs detected and disruption/risk mentioned in reasoning. Re-routing evaluation triggered."})
      
    if not should_evaluate_rerouting:
        full_contingency_plan.append({"Type": "Info", "Message": "No re-routing needed based on risk assessment."})
        return summary_status, full_contingency_plan, []
  
    if not emergency_skus:
        full_contingency_plan.append({"Type": "Info", "Message": "No emergency SKUs identified for re-routing consideration."})
        return "Re-routing evaluation needed (No Emergency SKUs)", full_contingency_plan, []
  
    shipments_found_for_emergency_skus = False
    re_routing_options_found = False
    emergency_sku_reroute_status = []
  
    for sku in emergency_skus:
        shipments = list(shipments_collection.find({"Product_SKU": sku, "Status": {"$in": ["Pending", "In Transit", "Out for Delivery"]}}))
      
        if not shipments:
            full_contingency_plan.append({"Type": "No Shipment", "SKU": sku, "Status": f"No active shipments found for emergency SKU {sku}."})
            emergency_sku_reroute_status.append({"SKU": sku, "Status": "No Active Shipment"})
            continue
        else:
            shipments_found_for_emergency_skus = True
          
        for shipment in shipments:
            shipment_id = shipment.get("Shipment_ID", "N/A")
            required_qty = shipment.get("Order_Volume", 0)
            dest_lat = shipment.get("Destination_Lat")
            dest_lon = shipment.get("Destination_Lon")
            original_cost = shipment.get("initial_shipping_cost", 0)
            original_tat_days = shipment.get("initial_delivery_tat_days", 0)
          
            if dest_lat is None or dest_lon is None:
                full_contingency_plan.append({"Type": "Shipment", "Shipment ID": shipment_id, "SKU": sku, "Status": "Missing destination coordinates."})
                emergency_sku_reroute_status.append({"SKU": sku, "Status": "No Optimal Route"})
                continue
          
            nearest_fcs_ids = get_nearest_fcs(dest_lat, dest_lon, fc_coordinates)
            found_alternative_for_this_shipment = False
            for nearby_fc_id in nearest_fcs_ids:
                nearby_fc_name = fc_id_to_name.get(nearby_fc_id, nearby_fc_id)
                nearby_fc_details_doc = fulfillment_centers_collection.find_one({"FC_ID": nearby_fc_id}) 
              
                if not nearby_fc_details_doc:
                    logger.warning(f"FC details not found in DB for nearby_fc_id: {nearby_fc_id}")
                    continue
              
                cost_multiplier = nearby_fc_details_doc.get("re_routing_cost_multiplier", 1.2)
                tat_adder = nearby_fc_details_doc.get("re_routing_tat_adder_days", 1)
                availability = check_inventory(nearby_fc_id, sku, required_qty)
                if availability >= 90:
                    re_routed_cost = original_cost * cost_multiplier
                    re_routed_tat_days = original_tat_days + tat_adder
                    cost_increase = re_routed_cost - original_cost
                    tat_delay_days = re_routed_tat_days - original_tat_days
                  
                    full_contingency_plan.append({
                        "Type": "Shipment",
                        "Shipment ID": shipment_id,
                        "SKU": sku,
                        "Re-routing Destination": nearby_fc_name,
                        "Inventory %": round(availability, 1),
                        "Original Cost": round(original_cost, 2),
                        "New Cost": round(re_routed_cost, 2),
                        "Cost Δ": round(cost_increase, 2),
                        "Original TAT": original_tat_days,
                        "New TAT": re_routed_tat_days,
                        "TAT Δ": tat_delay_days
                    })
                    re_routing_options_found = True
                    found_alternative_for_this_shipment = True
                    emergency_sku_reroute_status.append({"SKU": sku, "Status": "Re-routed"})
                    break
              
            if not found_alternative_for_this_shipment:
                full_contingency_plan.append({"Type": "Shipment", "Shipment ID": shipment_id, "SKU": sku, "Status": "No nearby FC with sufficient inventory within 150 miles to re-route."})
                emergency_sku_reroute_status.append({"SKU": sku, "Status": "No Optimal Route"})
              
    if re_routing_options_found:
        summary_status = "Re-routing options available"
    elif shipments_found_for_emergency_skus:
        summary_status = "Re-routing evaluation: No optimal routes found"
    else:
        summary_status = "No active emergency shipments found"
      
    return summary_status, full_contingency_plan, emergency_sku_reroute_status

# Check Inventory Availability
def check_inventory(fc_id, sku, required_qty):
    inventory_doc = inventory_collection.find_one({"FC_ID": fc_id, "Product_SKU": sku})
    if inventory_doc and "Quantity" in inventory_doc:
        available_qty = inventory_doc["Quantity"]
        return (available_qty / required_qty) * 100 if required_qty > 0 else 0.0
    return 0.0

# Function to get nearest FCs based on distance
def get_nearest_fcs(destination_lat, destination_lon, fc_coordinates):
    nearest_fcs = []
    for fc_id, fc_details in fc_coordinates.items():
        fc_lat, fc_lon = fc_details["coords"]
        distance = geodesic((destination_lat, destination_lon), (fc_lat, fc_lon)).miles
        if distance <= 150:
            nearest_fcs.append((fc_id, distance))
    nearest_fcs.sort(key=lambda x: x[1])
    return [fc[0] for fc in nearest_fcs]

# Helper function to get aggregated disruption data
def get_disruption_history_data():
    all_disruptions = []
    collections_to_check = [weather_collection, news_collection, social_media_collection, labor_collection, logistics_collection]
    
    thirty_days_ago = time.time() - (30 * 24 * 3600)

    for col in collections_to_check:
        docs = list(col.find({"timestamp": {"$gte": thirty_days_ago}}, {"location": 1, "timestamp": 1, "description": 1, "source": 1, "title": 1}))
        for doc in docs:
            disruption_text = doc.get('description', doc.get('title', 'No description'))
            all_disruptions.append({
                "date": datetime.fromtimestamp(doc['timestamp']).strftime('%Y-%m-%d'),
                "hour": datetime.fromtimestamp(doc['timestamp']).hour,
                "location": doc.get('location', 'General'),
                "type": doc.get('source', 'Unknown'),
                "description": disruption_text
            })
    
    if not all_disruptions:
        return pd.DataFrame(), pd.DataFrame()

    df_disruptions = pd.DataFrame(all_disruptions)
    df_disruptions['date'] = pd.to_datetime(df_disruptions['date'])
    df_disruptions['count'] = 1
    
    daily_disruptions = df_disruptions.groupby('date').size().reset_index(name='count')
    daily_disruptions.columns = ['Date', 'Disruption Count']
    
    return daily_disruptions, df_disruptions

# Cached FC Data Function
def get_fc_data(mode, selected_scenario):
  fcs, fc_to_city, fc_to_fc_id, fc_id_to_name, fc_coordinates = get_fcs()
  if not fcs:
    logger.info("No FCs to process, yielding empty list.")
    return
  
  risk_data = {}
  if mode == "Simulation Mode" and selected_scenario:
    scenario = scenarios[selected_scenario]
    affected_cities = scenario.get("affected_cities", [])
    affected_fcs = scenario.get("affected_fcs", [])
    event_type = scenario["event_type"]
    logger.info(f"Simulation Mode: Scenario={selected_scenario}, Event Type={event_type}, Affected Cities={affected_cities}")
  else:
    scenario = None
    event_type = None
    affected_cities = []
    affected_fcs = []
    logger.info("Real Mode: No simulation scenario applied.")
    
  current_time = time.time()
  time_threshold = current_time - 86400
  
  for fc in fcs:
    city = fc_to_city[fc]
    fc_id = fc_to_fc_id[fc]
    
    # Determine simulated data based on event_type only
    simulated_weather = get_simulated_weather(scenario, city) if mode == "Simulation Mode" and scenario else None
    simulated_social_media = get_simulated_social_media(scenario, city) if mode == "Simulation Mode" and scenario else None
    simulated_inventory = get_simulated_inventory(scenario, fc, fc_id) if mode == "Simulation Mode" and scenario else None
    simulated_labor = get_simulated_labor(scenario, city) if mode == "Simulation Mode" and scenario else None
    simulated_news = get_simulated_news(scenario, city) if mode == "Simulation Mode" and scenario else None
    simulated_logistics = get_simulated_logistics(scenario, city) if mode == "Simulation Mode" and scenario else None
    
    try:
      risk_prompt = generate_risk_prompt(
        fc_name=fc,
        city=city,
        fc_id=fc_id,
        event_type=event_type,
        simulated_weather=simulated_weather,
        simulated_social_media=simulated_social_media,
        simulated_inventory=simulated_inventory,
        simulated_news=simulated_news,
        simulated_labor=simulated_labor,
        simulated_logistics=simulated_logistics
      )
      # Get risk assessment from Gemini
      risk_score, status, reasoning, emergency_classifications_from_gemini = gemini_predict(risk_prompt, fc_name=fc)
      
      # Update risk_data BEFORE generating the contingency plan
      risk_data[fc] = {"Risk Score": risk_score, "Status": status, "Reasoning": reasoning}
      
      # Generate contingency plan with updated risk_data
      contingency_plan_summary, contingency_plan_full_detail, emergency_sku_reroute_status = generate_contingency_plan(
        fc, city, risk_score, risk_data, fc_coordinates,
        emergency_classifications_from_gemini,
        shipments_collection, fulfillment_centers_collection
      )
      
      # Store results in the database
      gemini_prompts_collection.insert_one({
        "fc_name": fc,
        "city": city,
        "prompt_text": risk_prompt,
        "timestamp": get_est_datetime(),
        "emergency_classifications": emergency_classifications_from_gemini,
        "reasoning": reasoning,
        "contingency_plan_full": contingency_plan_full_detail,
        "emergency_sku_reroute_status": emergency_sku_reroute_status
      })
      
      row = {
        "FC Name": f'<a href="?selected_fc={fc_id}&view=inventory">{fc}</a>',
        "City": city,
        "Risk Score": risk_score,
        "Status": status,
        "Contingency Plan": contingency_plan_summary,
        "Last Updated (EST)": get_est_datetime(),
        "Reasoning": f'<a href="?selected_fc={fc_id}&view=reasoning">View Reasoning</a>',
        "View Plan": f'<a href="?selected_fc={fc_id}&view=contingency_plan">View Plan</a>'
      }
      yield row
    except Exception as e:
      logger.error(f"Error processing FC {fc}: {str(e)}")
      row = {
        "FC Name": fc,
        "City": city,
        "Risk Score": 50,
        "Status": "Unknown",
        "Contingency Plan": "Error processing data",
        "Last Updated (EST)": get_est_datetime(),
        "Reasoning": f"Error: {str(e)}",
        "View Plan": "N/A"
      }
      yield row
      
      
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
    
    if selected_fc and selected_view == "inventory":
        fc_name = fc_id_to_name.get(selected_fc, "Unknown FC")
        st.write(f"Inventory for {fc_name} ({selected_fc})")
        inventory_docs = list(inventory_collection.find({"FC_ID": selected_fc}))
        if inventory_docs:
            emergency_inventory_count = 0
            non_emergency_inventory_count = 0
            
            inventory_sku_details = []
            for doc in inventory_docs:
                sku = doc.get("Product_SKU")
                quantity = doc.get("Quantity", 0)
                is_emergency = doc.get("Is_Emergency_Defined", False)
                
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

            st.dataframe(pd.DataFrame(inventory_docs), use_container_width=True)

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
                st.plotly_chart(fig_inv, use_container_width=True, key="inventory_pie")
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
    elif selected_fc and selected_view == "contingency_plan":
        fc_name = fc_id_to_name.get(selected_fc, "Unknown FC")
        st.write(f"Contingency Plan for {fc_name} ({selected_fc})")
        prompt_doc = gemini_prompts_collection.find_one({"fc_name": fc_name}, sort=[("timestamp", -1)])
        if prompt_doc and "contingency_plan_full" in prompt_doc:
            contingency_data = prompt_doc["contingency_plan_full"]
            
            if isinstance(contingency_data, str):
                st.markdown(contingency_data)
            else:
                rerouting_shipments = [d for d in contingency_data if d.get('Type') == 'Shipment' and 'Re-routing Destination' in d]
                if rerouting_shipments:
                    df_rerouting = pd.DataFrame(rerouting_shipments)
                    st.subheader("Shipments with Re-routing Options")
                    st.dataframe(df_rerouting[['Shipment ID', 'SKU', 'Re-routing Destination', 'Inventory %', 'Original Cost', 'New Cost', 'Cost Δ', 'Original TAT', 'New TAT', 'TAT Δ']])
                
                no_shipment_skus = [d for d in contingency_data if d.get('Type') == 'No Shipment']
                if no_shipment_skus:
                    df_no_shipment = pd.DataFrame(no_shipment_skus)
                    st.subheader("SKUs with No Active Shipments")
                    st.dataframe(df_no_shipment[['SKU', 'Status']])
                
                info_messages = [d['Message'] for d in contingency_data if d.get('Type') == 'Info']
                if info_messages:
                    st.subheader("Additional Information")
                    for msg in info_messages:
                        st.write(msg)
        else:
            st.write(f"No detailed contingency plan found for {fc_name} ({selected_fc})")
        if st.button("Back to Dashboard"):
            st.query_params["view"] = "dashboard"
            st.rerun()
    else:
        cache_key = f"{st.session_state.mode}_{selected_scenario}_{st.session_state.last_refresh}"
        
        summary_analytics_placeholder = st.empty()
        risk_pie_chart_placeholder = st.empty()
        disruption_bar_chart_placeholder = st.empty()
        table_display_placeholder = st.empty()

        if "fc_data_cache" not in st.session_state or st.session_state.get("fc_data_cache_key") != cache_key:
            st.session_state.fc_data_cache = []
            st.session_state.fc_data_cache_key = cache_key
            
            with st.spinner("Processing Fulfillment Centers..."):
                for row_data in get_fc_data(st.session_state.mode, selected_scenario):
                    st.session_state.fc_data_cache.append(row_data)
                    
                    df_display = pd.DataFrame(st.session_state.fc_data_cache)
                    
                    # Update Summary Analytics
                    total_fcs = len(df_display)
                    high_risk_fcs = len(df_display[df_display['Status'] == 'High Risk'])
                    medium_risk_fcs = len(df_display[df_display['Status'] == 'Medium Risk'])
                    low_risk_fcs = len(df_display[df_display['Status'] == 'Low Risk'])
                    contingency_options_fcs = len(df_display[
                        df_display['Contingency Plan'].str.contains("Re-routing options available|Re-routing evaluation needed", na=False)
                    ])
                    
                    with summary_analytics_placeholder.container():
                        st.subheader("FC Network Overview")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1: st.metric("Total FCs", total_fcs)
                        with col2: st.metric("High Risk FCs", high_risk_fcs)
                        with col3: st.metric("Medium Risk FCs", medium_risk_fcs)
                        with col4: st.metric("Low Risk FCs", low_risk_fcs)
                        st.markdown("---")
                    
                    # Update FC Risk Distribution Pie Chart
                    if not df_display.empty:
                        df_risk_counts = df_display.groupby('Status').size().reset_index(name='Count')
                        df_risk_counts['FCs'] = df_risk_counts['Status'].apply(lambda x: ', '.join(df_display[df_display['Status'] == x]['FC Name'].str.replace(r'<[^>]*>', '', regex=True).tolist()))
                        
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
                        with risk_pie_chart_placeholder.container():
                            st.subheader("FC Risk Distribution")
                            st.plotly_chart(fig_risk_pie, use_container_width=True, key=f"risk_pie_chart_dynamic_{len(df_display)}")
                            st.markdown("---")
                    
                    # Update Disruption History Bar Chart
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
                        with disruption_bar_chart_placeholder.container():
                            st.subheader("Recent Disruption History")
                            st.plotly_chart(fig_disruption_bar, use_container_width=True, key=f"disruption_bar_chart_dynamic_{len(df_display)}")
                            st.markdown("---")
                    
                    # Update Table Display
                    table_display_placeholder.markdown(df_display.to_html(escape=False), unsafe_allow_html=True)
        else:
            df_display = pd.DataFrame(st.session_state.fc_data_cache)

            total_fcs = len(df_display)
            high_risk_fcs = len(df_display[df_display['Status'] == 'High Risk'])
            medium_risk_fcs = len(df_display[df_display['Status'] == 'Medium Risk'])
            low_risk_fcs = len(df_display[df_display['Status'] == 'Low Risk'])
            contingency_options_fcs = len(df_display[
                df_display['Contingency Plan'].str.contains("Re-routing options available|Re-routing evaluation needed", na=False)
            ])

            st.subheader("FC Network Overview")
            col1, col2, col3, col4 = st.columns(4)
            with col1: st.metric("Total FCs", total_fcs)
            with col2: st.metric("High Risk FCs", high_risk_fcs)
            with col3: st.metric("Medium Risk FCs", medium_risk_fcs)
            with col4: st.metric("Low Risk FCs", low_risk_fcs)
            st.markdown("---")

            if not df_display.empty:
                df_risk_counts = df_display.groupby('Status').size().reset_index(name='Count')
                df_risk_counts['FCs'] = df_risk_counts['Status'].apply(lambda x: ', '.join(df_display[df_display['Status'] == x]['FC Name'].str.replace(r'<[^>]*>', '', regex=True).tolist()))
                
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
                st.plotly_chart(fig_risk_pie, use_container_width=True, key="risk_pie_chart_cached")
                st.markdown("---")

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
                st.plotly_chart(fig_disruption_bar, use_container_width=True, key="disruption_bar_chart_cached")
                st.markdown("---")

            table_display_placeholder.markdown(df_display.to_html(escape=False), unsafe_allow_html=True)
            
        if not st.session_state.fc_data_cache:
            st.warning("No data available to display. Please check the logs or ensure MongoDB is populated.")

except Exception as e:
    logger.error(f"Unexpected error: {str(e)}")
    st.error(f"An unexpected error occurred: {str(e)}. Please check the logs for details.")