# 🧠 Supply Sentinel – AI Agent to Predict Disruptions in Supply Chain  
**Course**: INFO 607 – Spring 2025  
**Team Members**: Arya Monani, Sushant Mahalle, Alan Uthuppan, Valentina Ozornina  

---

## 📌 Table of Contents
- [Problem Statement & Business Goals](#problem-statement--business-goals)  
- [Project Scope](#project-scope)  
- [System Architecture](#system-architecture)  
- [LLM Risk Prediction](#llm-risk-prediction)  
- [Contingency Planning](#contingency-planning)  
- [Dashboard Visualization](#dashboard-visualization)  
- [How to Connect to MongoDB Atlas](#how-to-connect-to-mongodb-atlas)  
- [Gemini API Integration](#gemini-api-integration)  
- [Software & Hardware Requirements](#software--hardware-requirements)  
- [Deliverables](#deliverables)  
- [References](#references)  
- [Team](#team)

---

## 🧩 Problem Statement & Business Goals

Modern supply chains rely on Fulfillment Centers (FCs) for last-mile delivery. A disruption at even one FC (e.g., strike, flood, or cyberattack) can cascade into delayed or failed deliveries. Most logistics systems today react after disruptions occur.

### 🎯 Business Goals
- Predict disruptions before they happen
- Reroute emergency shipments proactively
- Preserve SLAs by flagging risky FCs
- Prioritize emergency SKUs (e.g., medical, industrial)

### Key Questions:
- Which FCs are at risk?
- Which emergency shipments will be delayed?
- What reroutes are feasible?
- What are the cost/time tradeoffs?

---

## 📌 Project Scope

### ✅ In Scope:
- MongoDB Atlas to store real-time + synthetic data
- Gemini AI to assess risk and classify emergency SKUs
- Streamlit Dashboard for real-time + simulated views
- Synthetic data generation for FCs, shipments, inventory
- Proximity-based rerouting within 150 miles

### ❌ Out of Scope:
- Training custom ML models
- Real-time streaming (e.g., Kafka)
- Frontend frameworks (e.g., React)
- Enterprise MongoDB features (sharding, encryption)
- Retrieval-Augmented Generation (RAG)

---

## 🏗️ System Architecture

### 🔄 Workflow
1. **Data Ingestion**: Weather, News, Reddit, Logistics, Synthetic
2. **Storage**: MongoDB Atlas
3. **Risk Prediction**: Gemini LLM with structured prompt
4. **Contingency Logic**: Based on proximity and SKU availability
5. **Visualization**: Streamlit Dashboard

Collections: `fulfillment_centers`, `inventory`, `shipments`, `weather`, `news`, `social_media`, `logistics`, `gemini_prompts`

---

## 🧠 LLM Risk Prediction

### 🗂 Prompt Inputs:
- Recent weather in FC region
- Reddit/social sentiment
- News keywords (e.g., "strike", "delay")
- Inventory shortages
- Shipment status

### 🔎 Gemini Output:
- `Risk Score`: 0–100  
- `Status`: Low, Medium, High  
- `Reasoning`: Natural language summary  
- `Emergency SKUs`: List of flagged items for reroute

---

## 🔁 Contingency Planning

If an FC is flagged **High Risk**:
- System searches for FCs **within 150 miles**
- Alternate FC must have **≥90% of SKU quantity**
- Rerouting cost = base × multiplier  
- TAT (Turnaround Time) = base + delay days

If no reroute is possible, FC is flagged and alert issued.

---

## 📊 Dashboard Visualization

Built with **Streamlit**, this dashboard allows:
- Switching between **Real Mode** and **Simulation Mode**
- View FC map with risk-color coding
- See emergency SKUs and rerouting outcomes
- Plot time-series and pie charts (via Plotly)
- Inspect LLM prompts and structured output

---

## 🔌 How to Connect to MongoDB Atlas

### 1. `.env` Configuration
```env
MONGO_URI=mongodb+srv://<username>:<password>@cluster.mongodb.net/supply_chain_db
```

### 2. Connect in Python
```python
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"))
db = client["supply_chain_db"]
```

---

## 🤖 Gemini API Integration

### 1. Setup `.env`
```env
GEMINI_API_KEY=your-google-api-key
```

### 2. Install Dependencies
```bash
pip install google-generativeai
```

### 3. Generate Prediction
```python
import google.generativeai as genai

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel("gemini-1.5-flash")
response = model.generate_content(prompt)
```

---

## 💻 Software & Hardware Requirements

### 🧰 Software Stack
- Python 3.10+
- Streamlit
- pymongo
- google-generativeai
- plotly
- faker
- schedule
- requests, feedparser, praw

### 🖥️ Hardware
- Laptop/Desktop (min 8 GB RAM)
- Internet (for MongoDB & Gemini APIs)

### 🧪 Development Tools
- VS Code  
- GitHub  

---

## 📦 Deliverables

- MongoDB schema and database
- Streamlit dashboard for real-time + simulation
- Gemini-integrated Python risk agent
- Synthetic data generator for FCs and shipments
- Simulation scenarios (20+ cases)
- Final report and README documentation

---

## 📚 References

- [MongoDB Atlas Docs](https://www.mongodb.com/docs/atlas/)
- [Google Gemini API](https://ai.google.dev)
- [Reddit API (PRAW)](https://praw.readthedocs.io)
- [Streamlit Docs](https://docs.streamlit.io)
- [NewsAPI](https://newsapi.org)
- [Open-Meteo API](https://open-meteo.com/en/docs)
- [Faker Python](https://faker.readthedocs.io)

---

## 👥 Team

| Name               | Email               | Signature |
|--------------------|---------------------|-----------|
| Alan Uthuppan      | aju44@drexel.edu    | AJU       |
| Arya Monani        | am5446@drexel.edu   | AM        |
| Sushant Mahalle    | scm364@drexel.edu   | SCM       |
| Valentina Ozornina | vo55@drexel.edu     | VO        |

---

## ⚙️ Installation Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/supply-sentinel.git
cd supply-sentinel
```

### 2. Create and Activate Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

### 3. Install Required Packages
```bash
pip install -r requirements.txt
```

> If `requirements.txt` is missing, generate it using:
```bash
pip freeze > requirements.txt
```

---

## 🚀 How to Run the Project

### Step 1: Generate Synthetic Data
```bash
python dynamic_data_generation.py
```

### Step 2: (Optional) Pull Real-Time Data from APIs
```bash
python data_pull.py
```

### Step 3: Launch the Dashboard
```bash
streamlit run risk_prediction_dashboard.py
```

Then open your browser at [http://localhost:8501](http://localhost:8501)

---

## 🛡️ Environment Variables (.env)

Create a `.env` file in your root directory with the following:

```env
MONGO_URI=mongodb+srv://<username>:<password>@cluster.mongodb.net/supply_chain_db
GEMINI_API_KEY=your-google-api-key
REDDIT_CLIENT_ID=your-reddit-client-id
REDDIT_CLIENT_SECRET=your-reddit-client-secret
REDDIT_USER_AGENT=SupplySentinelBot/1.0
NEWS_API_KEY=your-newsapi-key
ATLAS_PUBLIC_KEY=your-atlas-public-key
ATLAS_PRIVATE_KEY=your-atlas-private-key
PROJECT_ID=your-mongodb-project-id
```

**Do not** commit your `.env` to source control.

---

## ❓ Troubleshooting

- **Streamlit app crashes**: Ensure your `.env` file is complete and correct.
- **MongoDB connection issues**: Check IP whitelist and credentials.
- **Gemini API error**: Make sure your API key is valid and has not exceeded quota.
- **No data appears in dashboard**: Run `dynamic_data_generation.py` to populate the database.

---
