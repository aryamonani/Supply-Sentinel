#!/usr/bin/env python3

import streamlit as st
import pandas as pd
from pymongo import MongoClient

# MongoDB connection (example setup)
client = MongoClient("mongodb://localhost:27017/")
db = client["supply_chain_db"]
inventory_collection = db["inventory"]

# Example FC mapping
fc_to_fc_id = {"New York FC 1": "NewYorkFC1", "Los Angeles FC 2": "LosAngelesFC2"}

# Cache FC data with 5-minute TTL
@st.cache_data(ttl=300)
def get_fc_data():
	# Simulated FC data
	return [
		{"FC Name": "New York FC 1", "City": "New York", "Risk Score": 0.8},
		{"FC Name": "Los Angeles FC 2", "City": "Los Angeles", "Risk Score": 0.6},
	]
	
# Check for selected FC
selected_fc = st.query_params.get("fc", None)

if selected_fc:
	# Fetch inventory for the selected FC
	inventory_data = list(inventory_collection.find({"FC_ID": selected_fc}))
	if inventory_data:
		df_inventory = pd.DataFrame(inventory_data)
		st.write(f"Inventory for {selected_fc}")
		st.dataframe(df_inventory)
	else:
		st.write(f"No inventory data found for {selected_fc}")
	if st.button("Back to Dashboard"):
		st.query_params.clear()
else:
	# Display dashboard table
	fc_data = get_fc_data()
	df = pd.DataFrame(fc_data)
	df["FC Name"] = df["FC Name"].apply(lambda x: f'<a href="?fc={fc_to_fc_id[x]}">{x}</a>')
	st.markdown(df.to_html(escape=False), unsafe_allow_html=True)