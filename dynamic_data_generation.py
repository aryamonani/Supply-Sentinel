#!/usr/bin/env python3

import random
from faker import Faker
from datetime import datetime, timedelta
import pytz
import time
import string
from pymongo import MongoClient
import requests
import logging
import base64
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Retrieve credentials from environment variables
ATLAS_PUBLIC_KEY = os.environ.get("ATLAS_PUBLIC_KEY")
ATLAS_PRIVATE_KEY = os.environ.get("ATLAS_PRIVATE_KEY")
PROJECT_ID = os.environ.get("PROJECT_ID")
MONGO_URI = os.environ.get("MONGO_URI")

'''# Function to get the user's public IP
def get_public_ip():
    try:
        response = requests.get("https://ifconfig.me", timeout=5)
        response.raise_for_status()
        public_ip = response.text.strip()
        logger.info(f"Public IP fetched: {public_ip}")
        return public_ip
    except Exception as e:
        logger.error(f"Failed to fetch public IP: {str(e)}")
        raise SystemExit(f"Exiting due to failure in fetching public IP: {str(e)}")

# Function to whitelist the IP in MongoDB Atlas
def whitelist_ip(ip_address):
    try:
        auth = base64.b64encode(f"{ATLAS_PUBLIC_KEY}:{ATLAS_PRIVATE_KEY}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json"
        }
        payload = [
            {
                "ipAddress": f"{ip_address}/32",
                "comment": "Dynamically whitelisted for Supply Sentinel project"
            }
        ]
        url = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{PROJECT_ID}/accessList"
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        logger.info(f"Successfully whitelisted IP: {ip_address}")
    except Exception as e:
        logger.error(f"Failed to whitelist IP {ip_address}: {str(e)}")
        # Continue execution; IP might already be whitelisted

# Whitelist the current IP before connecting to MongoDB
public_ip = get_public_ip()
whitelist_ip(public_ip)'''

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
fulfillment_centers_collection = db["fulfillment_centers"]
shipments_collection = db["shipments"]
inventory_collection = db["inventory"]

# Initialize Faker for realistic data
fake = Faker()

# Reduced FC List to 15 FCs (one per city, excluding "Washington")
fcs = [
    "New York FC 1", "Buffalo FC 2", "Rochester FC 3", "Newark FC 4", "Jersey City FC 5",
    "Paterson FC 6", "Philadelphia FC 7", "Pittsburgh FC 8", "Allentown FC 9", "Boston FC 10",
    "Worcester FC 11", "Springfield FC 12", "Baltimore FC 13", "Silver Spring FC 14", "Frederick FC 15"
]

# FC to City Mapping
fc_to_city = {fc: fc.split(" FC")[0] for fc in fcs}

# FC Name to FC_ID Mapping
fc_to_fc_id = {fc: f"{fc_to_city[fc].replace(' ', '')}FC{fc.split(' ')[-1]}" for fc in fcs}

# Mock Coordinates for Cities (approximate, for realism)
city_coordinates = {
    "New York": (40.7128, -74.0060), "Buffalo": (42.8864, -78.8784), "Rochester": (43.1566, -77.6088),
    "Newark": (40.7357, -74.1724), "Jersey City": (40.7178, -74.0431), "Paterson": (40.9168, -74.1718),
    "Philadelphia": (39.9526, -75.1652), "Pittsburgh": (40.4406, -79.9959), "Allentown": (40.6023, -75.4714),
    "Boston": (42.3601, -71.0589), "Worcester": (42.2626, -71.8023), "Springfield": (42.1015, -72.5898),
    "Baltimore": (39.2904, -76.6122), "Silver Spring": (38.9907, -77.0261), "Frederick": (39.4143, -77.4105),
    "Washington": (38.9072, -77.0369)  # Included for completeness, though no FC for Washington
}

# Possible Product Categories and Descriptions (Expanded to ensure more emergency categories)
product_data_templates = {
    "Health & Household": [
        {"desc": "First Aid Kit, 100 Pieces", "emergency_keyword": "First Aid Kit", "is_emergency": True},
        {"desc": "Hand Sanitizer, 16oz", "emergency_keyword": "Hand Sanitizer", "is_emergency": True},
        {"desc": "Vitamin C Supplements, 500mg", "emergency_keyword": "Vitamin C", "is_emergency": True},
        {"desc": "Bandages, Assorted Sizes", "emergency_keyword": "Bandages", "is_emergency": True},
        {"desc": "Face Masks, 50-pack", "emergency_keyword": "Face Masks", "is_emergency": True},
        {"desc": "Pain Relievers, 500 count", "emergency_keyword": "Pain Relievers", "is_emergency": True},
        {"desc": "Toothpaste, Mint Flavor", "emergency_keyword": None, "is_emergency": False}
    ],
    "Industrial & Scientific": [
        {"desc": "Safety Goggles, Anti-Fog", "emergency_keyword": "Safety Goggles", "is_emergency": True},
        {"desc": "Nitrile Gloves, 100-Pack", "emergency_keyword": "Nitrile Gloves", "is_emergency": True},
        {"desc": "Digital Thermometer", "emergency_keyword": "Digital Thermometer", "is_emergency": True},
        {"desc": "Heavy Duty Tarpaulin", "emergency_keyword": "Tarpaulin", "is_emergency": True},
        {"desc": "Work Gloves, Leather", "emergency_keyword": None, "is_emergency": False}
    ],
    "Grocery & Gourmet Food": [
        {"desc": "Organic Quinoa, 5lb Bag", "emergency_keyword": None, "is_emergency": False},
        {"desc": "Canned Soup, 12oz", "emergency_keyword": "Canned Soup", "is_emergency": True},
        {"desc": "Granola Bars, 12-Pack", "emergency_keyword": "Granola Bars", "is_emergency": True},
        {"desc": "Bottled Water, 24-pack", "emergency_keyword": "Bottled Water", "is_emergency": True},
        {"desc": "Dried Fruit Mix", "emergency_keyword": None, "is_emergency": False}
    ],
    "Electronics": [
        {"desc": "USB-C Cable, 6ft", "emergency_keyword": None, "is_emergency": False},
        {"desc": "Wireless Mouse", "emergency_keyword": None, "is_emergency": False},
        {"desc": "Portable Charger, 10000mAh", "emergency_keyword": "Portable Charger", "is_emergency": True},
        {"desc": "Battery Pack, AA", "emergency_keyword": "Battery Pack", "is_emergency": True}
    ],
    "Clothing, Shoes & Jewelry": [
        {"desc": "Men's T-Shirt, Medium", "emergency_keyword": None, "is_emergency": False},
        {"desc": "Running Shoes, Size 10", "emergency_keyword": None, "is_emergency": False},
        {"desc": "Winter Jacket", "emergency_keyword": "Winter Jacket", "is_emergency": True}
    ],
    "Home & Kitchen": [
        {"desc": "Non-Stick Frying Pan", "emergency_keyword": None, "is_emergency": False},
        {"desc": "Microfiber Towels, 6-Pack", "emergency_keyword": None, "is_emergency": False},
        {"desc": "LED Desk Lamp", "emergency_keyword": None, "is_emergency": False}
    ],
    "Toys & Games": [
        {"desc": "Board Game, Family Edition", "emergency_keyword": None, "is_emergency": False},
        {"desc": "Puzzle, 1000 Pieces", "emergency_keyword": None, "is_emergency": False},
        {"desc": "Action Figure Set", "emergency_keyword": None, "is_emergency": False}
    ]
}

# --- NEW: Generate a set of predefined products with fixed SKUs ---
# This ensures consistency between inventory and shipments
defined_products = []
for category, templates in product_data_templates.items():
    for template in templates:
        defined_products.append({
            "Product_SKU": f"SKU{random.randint(100000, 999999)}", # Fixed SKU for this product template
            "L1_Category": category,
            "Product_Description": template["desc"],
            "Is_Emergency": template["is_emergency"] # Explicitly mark for better control
        })

# Randomize FC order for distribution later
random.shuffle(fcs)

# Helper Functions
def generate_sku():
    """Generate a random SKU like B07CZQ56VA."""
    # This function is now mostly replaced by defined_products but kept for consistency if needed elsewhere.
    prefix = "B0" + random.choice(string.ascii_uppercase) + random.choice(string.digits)
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return prefix + suffix

def generate_inventory_id():
    """Generate a unique inventory ID."""
    return f"INV{random.randint(100000, 999999)}"

def generate_shipment_id():
    """Generate a unique shipment ID."""
    return f"S{random.randint(100, 999)}"

def get_est_datetime():
    """Get current time in EST."""
    est = pytz.timezone("America/New_York")
    return datetime.now(est)

# Generate Fulfillment Centers
def generate_fulfillment_centers():
    """Generate or update fulfillment center data."""
    for fc in fcs:
        fc_id = fc_to_fc_id[fc]
        city = fc_to_city[fc]
        base_lat, base_lon = city_coordinates[city]
        # Add small randomization to coordinates for uniqueness
        lat = base_lat + random.uniform(-0.01, 0.01)
        lon = base_lon + random.uniform(-0.01, 0.01)
        current_risk_score = random.randint(0, 100)
        max_risk_score = random.randint(5, 50)
        flagged = current_risk_score > 50  # Flag if high risk
        
        doc = {
            "FC_ID": fc_id,
            "FC_Name": fc,
            "city": city,  # Include city field for dashboard use
            "Latitude": lat,
            "Longitude": lon,
            "current_risk_score": current_risk_score,
            "max_risk_score": max_risk_score,
            "flagged": flagged
        }
        fulfillment_centers_collection.update_one(
            {"FC_ID": fc_id},
            {"$set": doc},
            upsert=True
        )
        logger.info(f"Updated/Inserted FC: {fc_id} in {city}")

# Generate Shipments
def generate_shipments():
    """Generate or update shipment data, prioritizing emergency items and relevant routes."""
    # Fetch existing FCs for source/destination selection
    all_fcs_data = list(fulfillment_centers_collection.find({}, {"FC_ID": 1, "city": 1, "Latitude": 1, "Longitude": 1}))
    if not all_fcs_data:
        logger.warning("No FCs found in DB to generate shipments against. Skipping shipment generation.")
        return

    num_shipments_to_generate = random.randint(10, 25) # Generate more shipments
    
    for i in range(num_shipments_to_generate):
        shipment_id = generate_shipment_id()
        
        # Determine if this shipment should be an emergency item (40% chance)
        is_emergency_shipment = random.random() < 0.4 

        if is_emergency_shipment:
            # Pick a random emergency product from our defined list
            eligible_products = [p for p in defined_products if p["Is_Emergency"]]
            if not eligible_products: continue # Skip if no emergency products defined
            chosen_product = random.choice(eligible_products)
        else:
            chosen_product = random.choice(defined_products)
            
        sku = chosen_product["Product_SKU"]
        category = chosen_product["L1_Category"]
        description = chosen_product["Product_Description"]

        # Choose a source FC (can be any FC for now)
        source_fc_doc = random.choice(all_fcs_data)
        source_fc_id = source_fc_doc["FC_ID"]
        source_lat, source_lon = source_fc_doc["Latitude"], source_fc_doc["Longitude"]

        # Determine destination - try to make it relevant for re-routing
        dest_fc_doc = random.choice(all_fcs_data) # Pick a random destination FC for coordinates
        dest_lat, dest_lon = dest_fc_doc["Latitude"], dest_fc_doc["Longitude"]
        dest_address = fake.address().split("\n")[0] + f", {dest_fc_doc['city']}, {fake.state_abbr()}"

        # Ensure order volume can be fulfilled by some FC
        order_volume = random.randint(20, 500)
        
        doc = {
            "Shipment_ID": shipment_id,
            "Product_SKU": sku,
            "L1_Category": category,
            "Product_Description": description, # Add description for context
            "Order_Volume": order_volume,
            "Status": random.choice(["Pending", "In Transit", "Out for Delivery"]), # Exclude Delivered for active status
            "Route_Type": random.choice(["inbound", "outbound"]),
            "Source_FC_ID": source_fc_id,
            "Source_Lat": source_lat,
            "Source_Lon": source_lon,
            "Destination_Lat": dest_lat,
            "Destination_Lon": dest_lon,
            "Destination_Address": dest_address,
            "Expected_Arrival": get_est_datetime() + timedelta(days=random.randint(1, 7)),
            "Is_Emergency_Product": chosen_product["Is_Emergency"] # Store this for easier lookup if needed
        }
        shipments_collection.update_one(
            {"Shipment_ID": shipment_id},
            {"$set": doc},
            upsert=True
        )
        logger.info(f"Updated/Inserted shipment: {shipment_id} ({sku}) for FC {source_fc_id}")

# Generate Inventory
def generate_inventory():
    """Generate or update inventory data based on defined products."""
    for fc in fcs:
        fc_id = fc_to_fc_id[fc]
        city = fc_to_city[fc]
        for product_template in defined_products:
            inventory_id = generate_inventory_id()
            
            doc = {
                "Inventory_ID": inventory_id,
                "FC_ID": fc_id,
                "L1_Category": product_template["L1_Category"],
                "Quantity": random.randint(50, 1000), # Ensure enough quantity for shipments
                "Product_SKU": product_template["Product_SKU"],
                "Product_Description": product_template["Product_Description"],
                "Is_Emergency_Defined": product_template["Is_Emergency"], # Store this too
                "Final_Delivery_Address": fake.address().split("\n")[0] + f", {city}, {fake.state_abbr()}"
            }
            inventory_collection.update_one(
                {"FC_ID": fc_id, "Product_SKU": product_template["Product_SKU"]}, # Use SKU for unique update
                {"$set": doc},
                upsert=True
            )
            logger.info(f"Updated/Inserted inventory: {product_template['Product_SKU']} for FC {fc_id}")

# Main Loop for Continuous Updates
def main():
    logger.info("Starting initial data generation for dynamic_data_generation.py...")
    generate_fulfillment_centers() # Ensure FCs exist first
    generate_inventory()
    generate_shipments()
    logger.info("Initial data generation complete. Starting continuous updates...")

    while True:
        logger.info(f"Generating incremental data at {get_est_datetime()}")
        generate_inventory() # Update/add new inventory
        generate_shipments() # Add new shipments
        logger.info("Incremental data generation complete, sleeping for 300 seconds...")
        time.sleep(300)  # Update every 5 minutes

if __name__ == "__main__":
    main()