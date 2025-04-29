# SupplySentinel/app/db/base.py
# Database connection setup (MongoDB client initialization)

import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the MongoDB connection string from the .env file
MONGO_URI = os.getenv("MONGO_URI")

# Validate the connection string
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable not set in .env file")

# Initialize the async MongoDB client
client = AsyncIOMotorClient(MONGO_URI)

# Access the supply_chain_db database
db = client["supply_chain_db"]

# Test the connection (async)
async def test_connection():
    try:
        await client.server_info()  # This will raise an exception if the connection fails
        print("Successfully connected to MongoDB")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        raise   
