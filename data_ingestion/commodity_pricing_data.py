import uuid
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

np.random.seed(42)

NUM_RECORDS = 100
NUM_DAYS = 30

commodities = {
    "Avocado": 250,
    "Mango": 120,
    "Banana": 45,
    "Apple": 180,
    "Orange": 90,
    "Grapes": 110,
    "Papaya": 55
}

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
end_date = pd.Timestamp.now().normalize()
commodity_names = list(commodities.keys())

for i in range(NUM_RECORDS):
    commodity = np.random.choice(commodity_names)
    base_price = commodities[commodity]
    market_id, city, lat, lon = markets[np.random.randint(0, len(markets))]

    price_date = end_date - pd.Timedelta(days=np.random.randint(0, NUM_DAYS))
    market_factor = np.random.uniform(0.85, 1.20)

    price = (
        base_price
        * market_factor
        * np.random.uniform(0.95, 1.05)
    )
    demand = np.random.choice(
        ["LOW", "MEDIUM", "HIGH"],
        p=[0.25, 0.50, 0.25]
    )
    supply = np.random.choice(
        ["LOW", "MEDIUM", "HIGH"],
        p=[0.25, 0.50, 0.25]
    )
    if demand == "HIGH":
        price *= 1.10
    if supply == "LOW":
        price *= 1.08

    quantity = np.random.uniform(500, 10000)
    records.append({
        "PRICE_ID": str(uuid.uuid4()),
        "PRICE_TIMESTAMP": price_date + pd.Timedelta(
            hours=np.random.randint(8, 19)
        ),
        "COMMODITY_NAME": commodity,
        "MARKET_ID": market_id,
        "MARKET_NAME": city + " Wholesale Market",
        "CITY": city,
        "COUNTRY": "India",
        "MARKET_LATITUDE": lat,
        "MARKET_LONGITUDE": lon,
        "PRICE_PER_KG": round(price, 2),
        "CURRENCY": "INR",
        "QUALITY_GRADE": np.random.choice(["A", "B", "C"]),
        "DEMAND_LEVEL": demand,
        "SUPPLY_LEVEL": supply,
        "AVAILABLE_QUANTITY_KG": round(quantity, 2),
        "DATA_SOURCE": "MOCK"
    })

df = pd.DataFrame(records)
conn = connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"]
)
cursor = conn.cursor()
print("Successfully connected to Snowflake")

create_table_sql = """
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
);
"""
cursor.execute(create_table_sql)
print("Snowflake table created")

success, nchunks, nrows, output = write_pandas(
    conn=conn,
    df=df,
    table_name="MOCK_COMMODITY_PRICING",
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
    auto_create_table=False,
    overwrite=False
)
print("Upload successful:", success)
print("Number of rows uploaded:", nrows)
cursor.close()
conn.close()