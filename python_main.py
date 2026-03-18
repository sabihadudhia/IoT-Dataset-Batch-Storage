import time
from pathlib import Path

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError


# 1) Connect to MongoDB with real retry + ping
MONGO_URI = "mongodb://admin:password@mongodb:27017/"
MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds

client = None
attempt = 1
while attempt <= MAX_RETRIES:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")  # forces real connection check
        print("Connected to MongoDB")
        break
    except Exception as e:
        print(f"Connection failed. Attempt {attempt}/{MAX_RETRIES}: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
        attempt += 1

if client is None:
    raise RuntimeError("Could not connect to MongoDB after retries")

# 2) Access the database and collection
db = client["sensor_db"]
collection = db["sensor_readings"]

# 3) Read the CSV file into a DataFrame
csv_path = Path(__file__).with_name("iot_telemetry_data.csv")
if not csv_path.exists():
    raise FileNotFoundError(f"CSV not found: {csv_path}")

df = pd.read_csv(csv_path)

if "ts" not in df.columns:
    raise ValueError("CSV must contain a 'ts' column")

# Ensure ts is numeric and clean invalid rows
df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
df = df.dropna(subset=["ts"]).copy()
df["ts"] = df["ts"].astype("int64")
df = df.sort_values("ts")

print(df.head())

print("Max LPG reading:", df['lpg'].max())
print("Max Smoke reading:", df['smoke'].max())

# 4) Batch insert logic
window_size = 300       # 5 minutes in seconds
start_time = int(df["ts"].min())
end_time = int(df["ts"].max())

current_window_start = start_time

while current_window_start <= end_time:
    current_window_end = current_window_start + window_size

    batch = df[(df["ts"] >= current_window_start) & (df["ts"] < current_window_end)]

    if not batch.empty:
        records = batch.to_dict("records")
        try:
            collection.insert_many(records, ordered=False)
            print(f"Inserted {len(records)} records")
        except PyMongoError as e:
            print(f"Insert failed for window {current_window_start}-{current_window_end}: {e}")

    current_window_start = current_window_end

client.close()