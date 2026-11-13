
import uuid
import os
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from snowflake.connector import connect

load_dotenv()
np.random.seed(42)

commodities = {"Avocado": 250, "Mango": 120, "Banana": 45, "Apple": 180, "Orange": 90, "Grapes": 110, "Papaya": 55}
markets = [
    ("MKT001", "Mumbai", 19.0760, 72.8777),
    ("MKT002", "Pune", 18.5204, 73.8567),
    ("MKT003", "Delhi", 28.6139, 77.2090),
    ("MKT004", "Hyderabad", 17.3850, 78.4867),
    ("MKT005", "Bengaluru", 12.9716, 77.5946),
    ("MKT006", "Chennai", 13.0827, 80.2707),
    ("MKT007", "Kolkata", 22.5726, 88.3639),
    ("MKT008", "Vijayawada", 16.5062, 80.6480)
]
records = []
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
for _ in range(100):
    commodity = np.random.choice(list(commodities.keys()))
    market_id, city, lat, lon = markets[np.random.randint(len(markets))]
    demand = np.random.choice(["LOW", "MEDIUM", "HIGH"], p=[0.25, 0.50, 0.25])
    supply = np.random.choice(["LOW", "MEDIUM", "HIGH"], p=[0.25, 0.50, 0.25])

    price = commodities[commodity] * np.random.uniform(0.85, 1.20) * np.random.uniform(0.95, 1.05)
    price *= 1.10 if demand == "HIGH" else 1
    price *= 1.08 if supply == "LOW" else 1

    records.append((
        str(uuid.uuid4()),
        today - timedelta(days=int(np.random.randint(30))) + timedelta(hours=int(np.random.randint(8, 19))),
        commodity,
        market_id,
        city + " Wholesale Market",
        city,
        "India",
        lat,
        lon,
        round(float(price), 2),
        "INR",
        np.random.choice(["A", "B", "C"]),
        demand,
        supply,
        round(float(np.random.uniform(500, 10000)), 2),
        "MOCK"
    ))
conn = connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"]
)
cursor = conn.cursor()
try:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MOCK_COMMODITY_PRICING (
            PRICE_ID VARCHAR(50),
            PRICE_TIMESTAMP TIMESTAMP_NTZ,
            COMMODITY_NAME VARCHAR(100),
            MARKET_ID VARCHAR(50),
            MARKET_NAME VARCHAR(100),
            CITY VARCHAR(100),
            COUNTRY VARCHAR(100),
            MARKET_LATITUDE NUMBER(10,6),
            MARKET_LONGITUDE NUMBER(10,6),
            PRICE_PER_KG NUMBER(10,2),
            CURRENCY VARCHAR(10),
            QUALITY_GRADE VARCHAR(20),
            DEMAND_LEVEL VARCHAR(20),
            SUPPLY_LEVEL VARCHAR(20),
            AVAILABLE_QUANTITY_KG NUMBER(15,2),
            DATA_SOURCE VARCHAR(50)
        )
    """)

    cursor.executemany("""
        INSERT INTO MOCK_COMMODITY_PRICING
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)
    """, records)
    conn.commit()
    print("Upload successful:", cursor.rowcount, "rows")
except Exception as e:
    conn.rollback()
    print("Error:", e)
finally:
    cursor.close()
    conn.close()