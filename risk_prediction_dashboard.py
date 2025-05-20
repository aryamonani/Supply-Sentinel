#!/usr/bin/env python3

from pymongo import MongoClient
import streamlit as st
from datetime import datetime
import pytz
import googlemaps
import requests
import pandas as pd

# MongoDB Setup with Retry Logic
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
	
# Real and Simulation Databases
REAL_DB_NAME = "supply_chain_db"
SIM_DB_NAME = "supply_chain_sim_db"

# Helper function to get EST datetime
def get_est_datetime():
	est = pytz.timezone("America/New_York")
	return datetime.now(est).isoformat()

# Define test case scenarios
test_scenarios = [
	{
		"name": "Hurricane in East Coast",
		"type": "weather",
		"affected_cities": ["New York", "Newark", "Philadelphia", "Boston", "Baltimore", "Washington"],
		"changes": {
			"weather": "hurricane",
			"temp": 25,
			"description": "Category 4 hurricane with heavy rain and high winds",
			"est_datetime": get_est_datetime()
		}
	},
	{
		"name": "Labor Strike in Chicago",
		"type": "labor",
		"affected_cities": ["Chicago"],
		"changes": {
			"description": "Workers on strike at major FCs",
			"severity": "high",
			"timestamp": get_est_datetime()
		}
	},
	{
		"name": "Low Inventory for Medical Supplies in Seattle",
		"type": "inventory",
		"affected_fcs": ["Seattle FC 3"],
		"changes": {
			"B07CZQ56VA": 0,  # Medical supplies SKU set to 0
			"B08XYZ1234": 10  # Another SKU set to low quantity
		}
	}
]

# Function to clear the simulation database
def clear_simulation_db():
	sim_db = client[SIM_DB_NAME]
	for collection in sim_db.list_collection_names():
		sim_db[collection].delete_many({})
		
# Function to apply a scenario's changes to the simulation database
def apply_scenario(scenario):
	sim_db = client[SIM_DB_NAME]
	if scenario["type"] == "weather":
		for city in scenario["affected_cities"]:
			weather_doc = {
				"location": city,
				"timestamp": datetime.now().isoformat(),
				"weather": scenario["changes"]["weather"],
				"temp": scenario["changes"]["temp"],
				"description": scenario["changes"]["description"],
				"est_datetime": scenario["changes"]["est_datetime"]
			}
			sim_db.weather.insert_one(weather_doc)
	elif scenario["type"] == "labor":
		for city in scenario["affected_cities"]:
			labor_doc = {
				"location": city,
				"timestamp": scenario["changes"]["timestamp"],
				"description": scenario["changes"]["description"],
				"severity": scenario["changes"]["severity"]
			}
			sim_db.labor.insert_one(labor_doc)
	elif scenario["type"] == "inventory":
		for fc in scenario["affected_fcs"]:
			for sku, quantity in scenario["changes"].items():
				inventory_doc = {
					"FC_ID": fc,
					"Product_SKU": sku,
					"Quantity": quantity,
					"L1_Category": "Medical Supplies" if sku == "B07CZQ56VA" else "General",
					"Product_Description": "Critical medical item" if sku == "B07CZQ56VA" else "General item"
				}
				sim_db.inventory.update_one(
					{"FC_ID": fc, "Product_SKU": sku},
					{"$set": inventory_doc},
					upsert=True
				)
				
# Streamlit Dashboard
st.title("Supply Chain FC Risk Dashboard")

# Simulation Mode Toggle
st.sidebar.title("Simulation Settings")
simulation_mode = st.sidebar.checkbox("Enable Simulation Mode", value=False)

if simulation_mode:
	# Select Test Scenarios
	selected_scenario_names = st.sidebar.multiselect(
		"Select Test Scenarios",
		[s["name"] for s in test_scenarios],
		help="Choose one or more scenarios to simulate disruptions."
	)
	selected_scenarios = [s for s in test_scenarios if s["name"] in selected_scenario_names]
	
	# Track previous scenarios to detect changes
	if "prev_scenarios" not in st.session_state:
		st.session_state.prev_scenarios = []
	if set(selected_scenario_names) != set(st.session_state.prev_scenarios):
		# Clear and repopulate simulation database
		clear_simulation_db()
		for scenario in selected_scenarios:
			apply_scenario(scenario)
		st.session_state.prev_scenarios = selected_scenario_names
		
	# Warning message for simulation mode
	st.warning("Simulation Mode is Enabled. Data shown is simulated based on selected scenarios.")
else:
	st.info("Real Mode: Displaying data from the production database.")
	
# Set the database based on simulation mode
db_name = SIM_DB_NAME if simulation_mode else REAL_DB_NAME
db = client[db_name]

# Define collections using the selected database
inventory_collection = db["inventory"]
shipments_collection = db["shipments"]
fulfillment_centers_collection = db["fulfillment_centers"]
gemini_prompts_collection = db["gemini_prompts"]

# Google Maps API Setup
GMAPS_API_KEY = "AIzaSyAdG-A50GffRPGyTZ8e7WT5TdPyIVouHYQ"
gmaps = googlemaps.Client(key=GMAPS_API_KEY)

# Gemini API Setup
GEMINI_API_KEY = "AIzaSyAiPBdPAvaAgBuuZ97CCCcJF4v4Z1AyWZA"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

# FC List
fcs = [
	"New York FC 1", "Buffalo FC 2", "Rochester FC 3", "Newark FC 4", "Jersey City FC 5",
	"Paterson FC 6", "Philadelphia FC 7", "Pittsburgh FC 8", "Allentown FC 9", "Boston FC 10",
	"Worcester FC 11", "Springfield FC 12", "Baltimore FC 13", "Silver Spring FC 14", "Frederick FC 15",
	"Washington FC 16", "Seattle FC 3"
]

# FC to City Mapping
fc_to_city = {fc: fc.split(" FC")[0] for fc in fcs}

# FC Name to FC_ID Mapping
fc_to_fc_id = {fc: f"{fc_to_city[fc].replace(' ', '')}FC{fc.split(' ')[-1]}" for fc in fcs}

# Reverse Mapping for FC_ID to FC Name
fc_id_to_name = {v: k for k, v in fc_to_fc_id.items()}

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
	"Seattle": []
}

# Gemini Prediction Function
def gemini_predict(prompt):
	try:
		url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
		headers = {"Content-Type": "application/json"}
		payload = {"contents": [{"parts": [{"text": prompt}]}]}
		response = requests.post(url, json=payload, headers=headers, timeout=10)
		response.raise_for_status()
		result = response.json()
		
		generated_text = result["candidates"][0]["content"]["parts"][0]["text"]
		lines = generated_text.split("\n")
		risk_score = 50
		status = "Unknown"
		reasoning = ""
		emergency_classifications = []
		
		for line in lines:
			if line.startswith("Risk Score:"):
				risk_score = float(line.split(": ")[1])
			elif line.startswith("Status:"):
				status = line.split(": ")[1]
			elif line.startswith("Reasoning:"):
				reasoning = line.split(": ")[1]
			elif line.startswith("- SKU:"):
				parts = line.split(", ")
				sku = parts[0].split(": ")[1]
				emergency = parts[1].split(": ")[1] == "True"
				reason = parts[2].split(": ")[1]
				emergency_classifications.append({"SKU": sku, "Emergency": emergency, "Reason": reason})
				
		return risk_score, status, reasoning, emergency_classifications
	except Exception as e:
		print(f"Gemini prediction error: {str(e)}")
		return 50, "Unknown", "Error in prediction", []
	
# Generate Enhanced Risk Prompt for Gemini
def generate_risk_prompt(fc_name, city):
	prompt = f"""
You are an AI expert in supply chain risk management for Amazon Fulfillment Centers (FCs). Your task is to:
1. Assess the risk of disruption for the {fc_name} located in {city} based on the provided data.
2. Determine if products in the FC's inventory belong to emergency categories critical for public health and safety.
3. Provide a risk score (0-100), status, reasoning, and emergency category classifications.

### Data for {fc_name} ({city})
#### Weather Data
"""
	weather_docs = list(db.weather.find({"location": city}).sort("timestamp", -1).limit(5))
	for doc in weather_docs:
		prompt += f"- {doc['est_datetime']}: Weather code {doc['weather']}, Temp {doc['temp']}°C, Conditions: {doc.get('description', 'N/A')}\n"
		
	prompt += """
#### Social Media (Reddit)
"""
	reddit_docs = list(db.social_media.find({"location": city}).sort("timestamp", -1).limit(5))
	for doc in reddit_docs:
		prompt += f"- r/{doc['subreddit']} ({doc['created_utc']}): \"{doc['text']}\" (Sentiment: {doc.get('sentiment', 'Neutral')})\n"
		
	prompt += """
#### News
"""
	news_docs = list(db.news.find({"location": city}).sort("timestamp", -1).limit(5))
	for doc in news_docs:
		prompt += f"- Reuters ({doc['timestamp']}): \"{doc['description']}\" (Impact: {doc.get('impact', 'Unknown')})\n"
		
	prompt += """
#### Labor
"""
	labor_docs = list(db.labor.find({"location": city}).sort("timestamp", -1).limit(5))
	for doc in labor_docs:
		prompt += f"- Reuters ({doc['timestamp']}): \"{doc['description']}\" (Severity: {doc.get('severity', 'Unknown')})\n"
		
	prompt += """
#### Logistics
"""
	logistics_docs = list(db.logistics.find({"location": city}).sort("timestamp", -1).limit(5))
	for doc in logistics_docs:
		prompt += f"- FreightWaves ({doc['est_datetime']}): \"{doc['description']}\" (Disruption Level: {doc.get('disruption_level', 'Unknown')})\n"
		
	prompt += """
#### Inventory (Sample Products)
"""
	fc_id = fc_to_fc_id[fc_name]
	inventory_docs = list(inventory_collection.find({"FC_ID": fc_id}).limit(5))
	for doc in inventory_docs:
		prompt += f"- SKU: {doc['Product_SKU']}, Category: {doc['L1_Category']}, Description: {doc['Product_Description']}, Quantity: {doc['Quantity']}\n"
		
	prompt += """
### Instructions
1. **Risk Assessment**:
	- Analyze the data to predict the risk of disruption (0-100).
	- Explain your reasoning for the risk score.

2. **Emergency Category Classification**:
	- For each product, determine if it’s an emergency item based on category and description.
	- Output SKUs with emergency status (True/False) and reasoning.

3. **Output Format**:
```
Risk Score: [0-100]
Status: [At Risk if risk score > 50, else Low Risk]
Reasoning: [Detailed explanation]
Emergency Classifications:
- SKU: [SKU], Emergency: [True/False], Reason: [Explanation]
...
```
"""
	return prompt
	
# Check Inventory Availability per SKU
def check_inventory(fc_id, sku, required_qty):
	inventory_doc = inventory_collection.find_one({"FC_ID": fc_id, "Product_SKU": sku})
	if inventory_doc:
		available_qty = inventory_doc["Quantity"]
		if available_qty >= required_qty:
			return 100.0
		return (available_qty / required_qty) * 100 if required_qty > 0 else 0.0
	return 0.0

# Generate Contingency Plan
def generate_contingency_plan(fc_name, city, risk_score, risk_data):
	if risk_score <= 50:
		return "No re-routing needed"
	
	fc_id = fc_to_fc_id[fc_name]
	prompt_doc = gemini_prompts_collection.find_one({"fc_name": fc_name}, sort=[("timestamp", -1)])
	emergency_skus = [c["SKU"] for c in prompt_doc.get("emergency_classifications", []) if c["Emergency"]] if prompt_doc else []
	
	shipments = list(shipments_collection.find({"Source_Center": fc_id, "Product_SKU": {"$in": emergency_skus}}))
	if not shipments:
		return "No emergency product shipments to re-route"
	
	contingency_plans = []
	for shipment in shipments:
		shipment_id = shipment["Shipment_ID"]
		sku = shipment["Product_SKU"]
		required_qty = shipment["Order_Volume"]
		
		dest_lat = shipment["Destination_Lat"]
		dest_lon = shipment["Destination_Lon"]
		delivery_location = f"{dest_lat},{dest_lon}"
		
		nearby_cities_list = nearby_cities.get(city, [])
		nearby_fcs = [f for f in fcs if fc_to_city[f] in nearby_cities_list and f != fc_name]
		
		distances = []
		for nearby_fc in nearby_fcs:
			try:
				nearby_fc_id = fc_to_fc_id[nearby_fc]
				nearby_fc_doc = fulfillment_centers_collection.find_one({"FC_ID": nearby_fc_id})
				if nearby_fc_doc:
					nearby_location = f"{nearby_fc_doc['Latitude']},{nearby_fc_doc['Longitude']}"
					result = gmaps.distance_matrix(origins=delivery_location, destinations=nearby_location, mode="driving", units="metric")
					if result['status'] != 'OK' or result['rows'][0]['elements'][0]['status'] != 'OK':
						distances.append((nearby_fc, float('inf'), float('inf'), float('inf')))
						continue
					distance_km = result['rows'][0]['elements'][0]['distance']['value'] / 1000
					distance_miles = distance_km * 0.621371
					duration_seconds = result['rows'][0]['elements'][0]['duration']['value']
					duration_hours = duration_seconds / 3600
					distances.append((nearby_fc, distance_km, distance_miles, duration_hours))
			except Exception as e:
				print(f"Distance error for {nearby_fc}: {str(e)}")
				distances.append((nearby_fc, float('inf'), float('inf'), float('inf')))
				
		distances = sorted(distances, key=lambda x: x[1])
		
		for nearby_fc, distance_km, distance_miles, duration_hours in distances:
			if distance_miles > 150:
				contingency_plans.append(f"Shipment {shipment_id} ({sku}): No viable solution found (nearest FC > 150 miles), refund the order")
				break
			
			nearby_risk_score = risk_data.get(nearby_fc, {}).get("Risk Score", 100)
			if nearby_risk_score <= 50:
				nearby_fc_id = fc_to_fc_id[nearby_fc]
				availability_percentage = check_inventory(nearby_fc_id, sku, required_qty)
				if availability_percentage >= 90:
					if availability_percentage < 100:
						contingency_plans.append(
							f"Shipment {shipment_id} ({sku}): Re-route to {nearby_fc} (Risk: {nearby_risk_score}, Distance: {distance_miles:.2f} miles, Travel Time: {duration_hours:.2f} hours, Inventory: {availability_percentage:.2f}% - Partial order sent, refund issued for remaining {100-availability_percentage:.2f}% of order)"
						)
					else:
						contingency_plans.append(
							f"Shipment {shipment_id} ({sku}): Re-route to {nearby_fc} (Risk: {nearby_risk_score}, Distance: {distance_miles:.2f} miles, Travel Time: {duration_hours:.2f} hours, Inventory: 100% - Full order sent)"
						)
					break
		else:
			contingency_plans.append(f"Shipment {shipment_id} ({sku}): No nearby FC with low risk and sufficient inventory within 150 miles, refund the order")
			
	return "\n".join(contingency_plans) if contingency_plans else "No emergency product shipments to re-route"

# Cached Function to Get FC Data
@st.cache_data(ttl=300)
def get_fc_data():
	risk_data = {}
	for fc in fcs:
		city = fc_to_city[fc]
		risk_prompt = generate_risk_prompt(fc, city)
		risk_score, status, reasoning, emergency_classifications = gemini_predict(risk_prompt)
		prompt_id = f"prompt_{int(datetime.now().timestamp())}"
		gemini_prompts_collection.insert_one({
			"prompt_id": prompt_id,
			"fc_name": fc,
			"city": city,
			"prompt_text": risk_prompt,
			"timestamp": get_est_datetime(),
			"emergency_classifications": emergency_classifications,
			"reasoning": reasoning
		})
		risk_data[fc] = {"Risk Score": risk_score, "Status": status, "Reasoning": reasoning}
		
	fc_data = []
	for fc in fcs:
		city = fc_to_city[fc]
		risk_score = risk_data[fc]["Risk Score"]
		status = risk_data[fc]["Status"]
		contingency_plan = generate_contingency_plan(fc, city, risk_score, risk_data)
		last_updated = get_est_datetime()
		fc_data.append({
			"FC Name": fc,
			"City": city,
			"Risk Score": risk_score,
			"Status": status,
			"Contingency Plan": contingency_plan,
			"Last Updated (EST)": last_updated,
			"Reasoning": risk_data[fc]["Reasoning"]
		})
		fc_id = fc_to_fc_id[fc]
		fulfillment_centers_collection.update_one(
			{"FC_ID": fc_id},
			{"$set": {
				"current_risk_score": risk_score,
				"status": status,
				"contingency_plan": contingency_plan,
				"last_updated": last_updated,
				"reasoning": risk_data[fc]["Reasoning"]
			}},
			upsert=True
		)
	return sorted(fc_data, key=lambda x: x["Risk Score"], reverse=True)

# Main Dashboard Logic
selected_fc = st.query_params.get("selected_fc", None)
selected_view = st.query_params.get("view", "dashboard")

if selected_fc and selected_view == "inventory":
	fc_name = fc_id_to_name.get(selected_fc, "Unknown FC")
	st.write(f"Inventory for {fc_name} ({selected_fc})")
	inventory_docs = list(inventory_collection.find({"FC_ID": selected_fc}))
	if inventory_docs:
		df_inventory = pd.DataFrame(inventory_docs)
		st.dataframe(df_inventory)
	else:
		st.write(f"No inventory found for {fc_name} ({selected_fc})")
	if st.button("Back to Dashboard"):
		st.experimental_set_query_params(view="dashboard")
elif selected_fc and selected_view == "reasoning":
	fc_name = fc_id_to_name.get(selected_fc, "Unknown FC")
	st.write(f"Reasoning for {fc_name} ({selected_fc})")
	prompt_doc = gemini_prompts_collection.find_one({"fc_name": fc_name}, sort=[("timestamp", -1)])
	if prompt_doc and "reasoning" in prompt_doc:
		st.write(prompt_doc["reasoning"])
	else:
		st.write(f"No reasoning found for {fc_name} ({selected_fc})")
	if st.button("Back to Dashboard"):
		st.experimental_set_query_params(view="dashboard")
else:
	st.write("Showing real-time FC risk statuses and contingency plans for emergency products. Data refreshes every 5 minutes.")
	if st.button("Refresh Data"):
		st.cache_data.clear()
		st.rerun()
	fc_data = get_fc_data()
	for item in fc_data:
		fc_name = item["FC Name"]
		fc_id = fc_to_fc_id[fc_name]
		item["FC Name"] = f'<a href="?selected_fc={fc_id}&view=inventory">{fc_name}</a>'
		item["Reasoning"] = f'<a href="?selected_fc={fc_id}&view=reasoning">View Reasoning</a>'
	df = pd.DataFrame(fc_data)
	html_table = df.to_html(escape=False)
	st.markdown(html_table, unsafe_allow_html=True)