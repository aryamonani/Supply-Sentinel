<<<<<<< HEAD
# SupplySentinel/app/main.py
# Application entry point: launches FastAPI backend and NiceGUI frontend

from fastapi import FastAPI
import nicegui
import asyncio
from app.db.base import db, test_connection
from app.db.crud import get_shipments_by_fc, get_fc_details, update_fc_risk_score

app = FastAPI(title="SupplySentinel")

# Test the MongoDB connection on startup
@app.on_event("startup")
async def startup_event():
    await test_connection()

# Basic route to verify the API is running
@app.get("/")
async def root():
    return {"message": "Welcome to SupplySentinel"}

# Test route to get shipments by fulfillment center
@app.get("/shipments/{fc_id}")
async def read_shipments(fc_id: str):
    shipments = await get_shipments_by_fc(fc_id)
    return {"fulfillment_center": fc_id, "shipments": shipments}

# Test route to get fulfillment center details
@app.get("/fc/{fc_id}")
async def read_fc(fc_id: str):
    fc = await get_fc_details(fc_id)
    return {"fulfillment_center": fc}

# Test route to update fulfillment center risk score
@app.post("/fc/{fc_id}/risk/{risk_score}")
async def update_risk(fc_id: str, risk_score: float):
    updated_fc = await update_fc_risk_score(fc_id, risk_score)
    return {"fulfillment_center": updated_fc}

# Launch NiceGUI frontend (to be implemented in dashboard/routes.py)
if __name__ == "__main__":
    nicegui.run(app=app, port=8080)
=======
import asyncio
from app.agent.background_agent import start_agent_loop

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_agent_loop())
>>>>>>> main
