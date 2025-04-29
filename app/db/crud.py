# SupplySentinel/app/db/crud.py
# CRUD operations for interacting with the database

from .base import db

# Retrieve all shipments for a specific fulfillment center
async def get_shipments_by_fc(fc_id: str):
    """
    Retrieve all shipments for a given fulfillment center.
    
    Args:
        fc_id (str): The ID of the fulfillment center (e.g., "LAX1").
    
    Returns:
        list: A list of shipment documents.
    """
    return await db.shipments.find({"Fulfillment_Center": fc_id}).to_list(None)

# Retrieve fulfillment center details by FC_ID
async def get_fc_details(fc_id: str):
    """
    Retrieve details of a fulfillment center by its FC_ID.
    
    Args:
        fc_id (str): The ID of the fulfillment center (e.g., "LAX1").
    
    Returns:
        dict: The fulfillment center document, or None if not found.
    """
    return await db.fc_details.find_one({"FC_ID": fc_id})

# Update the Risk_Score of a fulfillment center
async def update_fc_risk_score(fc_id: str, risk_score: float):
    """
    Update the Risk_Score of a fulfillment center.
    
    Args:
        fc_id (str): The ID of the fulfillment center (e.g., "LAX1").
        risk_score (float): The new risk score to set.
    
    Returns:
        dict: The updated fulfillment center document, or None if not found.
    """
    result = await db.fc_details.find_one_and_update(
        {"FC_ID": fc_id},
        {"$set": {"Risk_Score": risk_score}},
        return_document=True
    )
    return result
