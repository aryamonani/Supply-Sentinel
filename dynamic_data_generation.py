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

# City to State Mapping
city_to_state = {
    "New York": "NY",
    "Buffalo": "NY",
    "Rochester": "NY",
    "Newark": "NJ",
    "Jersey City": "NJ",
    "Paterson": "NJ",
    "Philadelphia": "PA",
    "Pittsburgh": "PA",
    "Allentown": "PA",
    "Boston": "MA",
    "Worcester": "MA",
    "Springfield": "MA",
    "Baltimore": "MD",
    "Silver Spring": "MD",
    "Frederick": "MD",
    "Washington": "DC"
}

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

# Possible Product Categories and Descriptions
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

# Generate a set of predefined products with fixed SKUs
defined_products = []
for category, templates in product_data_templates.items():
    for template in templates:
        defined_products.append({
            "Product_SKU": f"SKU{random.randint(100000, 999999)}",
            "L1_Category": category,
            "Product_Description": template["desc"],
            "Is_Emergency": template["is_emergency"]
        })

# Randomize FC order for distribution
random.shuffle(fcs)

# Helper Functions
def generate_sku():
    prefix = "B0" + random.choice(string.ascii_uppercase) + random.choice(string.digits)
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return prefix + suffix

def generate_inventory_id():
    return f"INV{random.randint(100000, 999999)}"

def generate_shipment_id():
    return f"S{random.randint(100, 999)}"

def get_est_datetime():
    est = pytz.timezone("America/New_York")
    return datetime.now(est).strftime("%Y-%m-%d %H:%M:%S")

# Generate Fulfillment Centers
def generate_fulfillment_centers():
    for fc in fcs:
        fc_id = fc_to_fc_id[fc]
        city = fc_to_city[fc]
        base_lat, base_lon = city_coordinates[city]
        lat = base_lat + random.uniform(-0.01, 0.01)
        lon = base_lon + random.uniform(-0.01, 0.01)
        current_risk_score = random.randint(0, 100)
        max_risk_score = random.randint(5, 50)
        flagged = current_risk_score > 50
        
        doc = {
            "FC_ID": fc_id,
            "FC_Name": fc,
            "city": city,
            "Latitude": lat,
            "Longitude": lon,
            "current_risk_score": current_risk_score,
            "max_risk_score": max_risk_score,
            "flagged": flagged,
            "re_routing_cost_multiplier": random.uniform(1.1, 1.5),
            "re_routing_tat_adder_days": random.randint(1, 3)
        }
        fulfillment_centers_collection.update_one(
            {"FC_ID": fc_id},
            {"$set": doc},
            upsert=True
        )
        logger.info(f"Updated/Inserted FC: {fc_id} in {city}")

# Generate Shipments
def generate_shipments():
    all_fcs_data = list(fulfillment_centers_collection.find({}, {"FC_ID": 1, "city": 1, "Latitude": 1, "Longitude": 1}))
    if not all_fcs_data:
        logger.warning("No FCs found in DB to generate shipments against. Skipping shipment generation.")
        return

    all_cities = list(city_coordinates.keys())  # All possible destination cities
    num_shipments_to_generate = random.randint(10, 25)
    
    for i in range(num_shipments_to_generate):
        shipment_id = generate_shipment_id()
        
        is_emergency_shipment = random.random() < 0.4
        if is_emergency_shipment:
            eligible_products = [p for p in defined_products if p["Is_Emergency"]]
            if not eligible_products: continue
            chosen_product = random.choice(eligible_products)
        else:
            chosen_product = random.choice(defined_products)
            
        sku = chosen_product["Product_SKU"]
        category = chosen_product["L1_Category"]
        description = chosen_product["Product_Description"]

        source_fc_doc = random.choice(all_fcs_data)
        source_fc_id = source_fc_doc["FC_ID"]
        source_lat, source_lon = source_fc_doc["Latitude"], source_fc_doc["Longitude"]

        dest_city = random.choice(all_cities)
        dest_state = city_to_state[dest_city]
        base_lat, base_lon = city_coordinates[dest_city]
        dest_lat = base_lat + random.uniform(-0.01, 0.01)
        dest_lon = base_lon + random.uniform(-0.01, 0.01)
        dest_address = fake.address().split("\n")[0] + f", {dest_city}, {dest_state}"

        order_volume = random.randint(20, 500)
        
        doc = {
            "Shipment_ID": shipment_id,
            "Product_SKU": sku,
            "L1_Category": category,
            "Product_Description": description,
            "Order_Volume": order_volume,
            "Status": random.choice(["Pending", "In Transit", "Out for Delivery"]),
            "Route_Type": random.choice(["inbound", "outbound"]),
            "Source_FC_ID": source_fc_id,
            "Source_Lat": source_lat,
            "Source_Lon": source_lon,
            "Destination_Lat": dest_lat,
            "Destination_Lon": dest_lon,
            "Destination_Address": dest_address,
            "Expected_Arrival": get_est_datetime(),
            "Is_Emergency_Product": chosen_product["Is_Emergency"],
            "initial_shipping_cost": random.uniform(50.0, 500.0),
            "initial_delivery_tat_days": random.randint(1, 7)
        }
        shipments_collection.update_one(
            {"Shipment_ID": shipment_id},
            {"$set": doc},
            upsert=True
        )
        logger.info(f"Updated/Inserted shipment: {shipment_id} ({sku}) for FC {source_fc_id}")

# Generate Inventory
def generate_inventory():
    cities = list(city_coordinates.keys())  # All possible cities
    for fc in fcs:
        fc_id = fc_to_fc_id[fc]
        for product_template in defined_products:
            inventory_id = generate_inventory_id()
            random_city = random.choice(cities)
            random_state = city_to_state[random_city]
            doc = {
                "Inventory_ID": inventory_id,
                "FC_ID": fc_id,
                "L1_Category": product_template["L1_Category"],
                "Quantity": random.randint(50, 1000),
                "Product_SKU": product_template["Product_SKU"],
                "Product_Description": product_template["Product_Description"],
                "Is_Emergency_Defined": product_template["Is_Emergency"],
                "Final_Delivery_Address": fake.address().split("\n")[0] + f", {random_city}, {random_state}"
            }
            inventory_collection.update_one(
                {"FC_ID": fc_id, "Product_SKU": product_template["Product_SKU"]},
                {"$set": doc},
                upsert=True
            )
            logger.info(f"Updated/Inserted inventory: {product_template['Product_SKU']} for FC {fc_id}")

# Main Loop for Continuous Updates
def main():
    logger.info("Starting initial data generation for dynamic_data_generation.py...")
    generate_fulfillment_centers()
    generate_inventory()
    generate_shipments()
    logger.info("Initial data generation complete. Starting continuous updates...")

    while True:
        logger.info(f"Generating incremental data at {get_est_datetime()}")
        generate_inventory()
        generate_shipments()
        logger.info("Incremental data generation complete, sleeping for 300 seconds...")
        time.sleep(300)

if __name__ == "__main__":
    main()