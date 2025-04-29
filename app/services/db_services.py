from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime
from pymongo.server_api import ServerApi

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')

client = MongoClient(MONGO_URI)
db = client['supplysentinel']
fulfillment_centers = db['fulfillment_center']  
risk_snapshots_collection = db['risk_snapshots']

def fetch_all_centers():
    return list(fulfillment_centers.find())

def update_facility_risk(fc_id: str, new_risk: int):
    fulfillment_centers.update_one(
        {"FC_ID": fc_id},
        {"$set": {"Risk_Score": new_risk}}
    )

def fetch_at_risk_facilities(threshold=30):
    return list(fulfillment_centers.find({"Risk_Score": {"$gt": threshold}})) 

def save_risk_snapshot(data: dict):
    risk_snapshots_collection.insert_one(data)
