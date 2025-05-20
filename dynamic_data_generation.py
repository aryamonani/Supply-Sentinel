#!/usr/bin/env python3

from pymongo import MongoClient
import random
from faker import Faker
from datetime import datetime, timedelta
import pytz
import time
import string

# Initialize Faker for realistic data
fake = Faker()

# MongoDB Setup
MONGO_URI = "mongodb+srv://arya-monani:t7gc2tHqtwli41sk@cluster0.cprme1k.mongodb.net/"
try:
	client = MongoClient(
		MONGO_URI,
		serverSelectionTimeoutMS=30000,
		connectTimeoutMS=30000,
		retryWrites=True,
		retryReads=True
	)
	client.server_info()
	print("Successfully connected to MongoDB Atlas")
except Exception as e:
	print(f"Failed to connect to MongoDB Atlas: {str(e)}")
	raise
	
db = client["supply_chain_db"]
fulfillment_centers_collection = db["fulfillment_centers"]
shipments_collection = db["shipments"]
inventory_collection = db["inventory"]

# FC List and Mappings (from your code)
fcs = [
	"New York FC 1", "Buffalo FC 2", "Rochester FC 3", "Newark FC 4", "Jersey City FC 5",
	"Paterson FC 6", "Philadelphia FC 7", "Pittsburgh FC 8", "Allentown FC 9", "Boston FC 10",
	"Worcester FC 11", "Springfield FC 12", "Baltimore FC 13", "Silver Spring FC 14", "Frederick FC 15",
	"Washington FC 16", "New York FC 17", "Buffalo FC 18", "Rochester FC 19", "Newark FC 20",
	"Jersey City FC 21", "Paterson FC 22", "Philadelphia FC 23", "Pittsburgh FC 24", "Allentown FC 25",
	"Boston FC 26", "Worcester FC 27", "Springfield FC 28", "Baltimore FC 29", "Silver Spring FC 30",
	"Frederick FC 31", "Washington FC 32", "New York FC 33", "Buffalo FC 34", "Rochester FC 35",
	"Newark FC 36", "Jersey City FC 37", "Paterson FC 38", "Philadelphia FC 39", "Pittsburgh FC 40",
	"Allentown FC 41", "Boston FC 42", "Worcester FC 43", "Springfield FC 44", "Baltimore FC 45",
	"Silver Spring FC 46", "Frederick FC 47", "Washington FC 48", "New York FC 49", "Buffalo FC 50",
	"Rochester FC 51", "Newark FC 52", "Jersey City FC 53", "Paterson FC 54", "Philadelphia FC 55"
]
fc_to_city = {fc: fc.split(" FC")[0] for fc in fcs}
fc_to_fc_id = {fc: f"{fc_to_city[fc].replace(' ', '')}FC{fc.split(' ')[-1]}" for fc in fcs}

# Mock Coordinates for Cities (approximate, for realism)
city_coordinates = {
	"New York": (40.7128, -74.0060), "Buffalo": (42.8864, -78.8784), "Rochester": (43.1566, -77.6088),
	"Newark": (40.7357, -74.1724), "Jersey City": (40.7178, -74.0431), "Paterson": (40.9168, -74.1718),
	"Philadelphia": (39.9526, -75.1652), "Pittsburgh": (40.4406, -79.9959), "Allentown": (40.6023, -75.4714),
	"Boston": (42.3601, -71.0589), "Worcester": (42.2626, -71.8023), "Springfield": (42.1015, -72.5898),
	"Baltimore": (39.2904, -76.6122), "Silver Spring": (38.9907, -77.0261), "Frederick": (39.4143, -77.4105),
	"Washington": (38.9072, -77.0369)
}

# Possible Product Categories
product_categories = [
	"Health & Household", "Industrial & Scientific", "Grocery & Gourmet Food",
	"Electronics", "Clothing, Shoes & Jewelry", "Home & Kitchen", "Toys & Games"
]

# Product Descriptions by Category
product_descriptions = {
	"Health & Household": ["First Aid Kit, 100 Pieces", "Hand Sanitizer, 16oz", "Vitamin C Supplements, 500mg"],
	"Industrial & Scientific": ["Safety Goggles, Anti-Fog", "Nitrile Gloves, 100-Pack", "Digital Thermometer"],
	"Grocery & Gourmet Food": ["Organic Quinoa, 5lb Bag", "Canned Soup, 12oz", "Granola Bars, 12-Pack"],
	"Electronics": ["USB-C Cable, 6ft", "Wireless Mouse", "Portable Charger, 10000mAh"],
	"Clothing, Shoes & Jewelry": ["Men's T-Shirt, Medium", "Running Shoes, Size 10", "Winter Jacket"],
	"Home & Kitchen": ["Non-Stick Frying Pan", "Microfiber Towels, 6-Pack", "LED Desk Lamp"],
	"Toys & Games": ["Board Game, Family Edition", "Puzzle, 1000 Pieces", "Action Figure Set"]
}

# Helper Functions
def generate_sku():
	"""Generate a random SKU like B07CZQ56VA."""
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
		
# Generate Shipments
def generate_shipments():
	"""Generate or update shipment data."""
	for _ in range(random.randint(5, 15)):  # Generate 5-15 new shipments per cycle
		fc = random.choice(fcs)
		fc_id = fc_to_fc_id[fc]
		city = fc_to_city[fc]
		category = random.choice(product_categories)
		shipment_id = generate_shipment_id()
		sku = generate_sku()
		source_lat, source_lon = city_coordinates[city]
		# Randomize destination coordinates slightly
		dest_lat = source_lat + random.uniform(-0.5, 0.5)
		dest_lon = source_lon + random.uniform(-0.5, 0.5)
		dest_address = fake.address().split("\n")[0] + f", {city}, {fake.state_abbr()}"
		expected_arrival = get_est_datetime() + timedelta(days=random.randint(1, 7))
	
		doc = {
			"Shipment_ID": shipment_id,
			"Product_SKU": sku,
			"L1_Category": category,
			"Order_Volume": random.randint(50, 1000),
			"Status": random.choice(["Pending", "In Transit", "Out for Delivery", "Delivered"]),
			"Route_Type": random.choice(["inbound", "outbound"]),
			"Source_Center": fc_id,
			"Source_Lat": source_lat + random.uniform(-0.01, 0.01),
			"Source_Lon": source_lon + random.uniform(-0.01, 0.01),
			"Destination_Lat": dest_lat,
			"Destination_Lon": dest_lon,
			"Destination_Address": dest_address,
			"Expected_Arrival": expected_arrival
		}
		shipments_collection.update_one(
			{"Shipment_ID": shipment_id},
			{"$set": doc},
			upsert=True
		)
	
# Generate Inventory
def generate_inventory():
	"""Generate or update inventory data."""
	for fc in fcs:
		fc_id = fc_to_fc_id[fc]
		city = fc_to_city[fc]
		for category in product_categories:
			inventory_id = generate_inventory_id()
			sku = generate_sku()
			description = random.choice(product_descriptions[category])
			dest_address = fake.address().split("\n")[0] + f", {city}, {fake.state_abbr()}"
			
			doc = {
				"Inventory_ID": inventory_id,
				"FC_ID": fc_id,
				"L1_Category": category,
				"Quantity": random.randint(100, 2000),
				"Product_SKU": sku,
				"Product_Description": description,
				"Final_Delivery_Address": dest_address
			}
			inventory_collection.update_one(
				{"Inventory_ID": inventory_id},
				{"$set": doc},
				upsert=True
			)
			
# Main Loop for Continuous Updates
def main():
	while True:
		print(f"Generating data at {get_est_datetime()}")
		generate_fulfillment_centers()
		generate_shipments()
		generate_inventory()
		print("Data generation complete, sleeping for 300 seconds...")
		time.sleep(300)  # Update every 5 minutes
		
if __name__ == "__main__":
	main()