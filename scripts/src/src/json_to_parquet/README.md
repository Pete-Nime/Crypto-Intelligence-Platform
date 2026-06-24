
### ============================================================
### Project: Crypto Intelligence Platform
### File: src/json_to_parquet.py
#
### Purpose:
### Converts raw JSON Lines crypto trade data into Parquet format.
#
### Pipeline:
### JSONL → Pandas DataFrame → Parquet
### ============================================================

import pandas as pd
from pathlib import Path


INPUT_FILE = Path("data/crypto_trades.jsonl")
OUTPUT_FILE = Path("data/crypto_trades.parquet")


if not INPUT_FILE.exists():
    raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")


df = pd.read_json(INPUT_FILE, lines=True)

df["trade_time"] = pd.to_datetime(df["trade_time"])
df["ingestion_time"] = pd.to_datetime(df["ingestion_time"])
df["consumer_received_time"] = pd.to_datetime(df["consumer_received_time"])

df["trade_date"] = df["trade_time"].dt.date
df["trade_hour"] = df["trade_time"].dt.hour
df["trade_minute"] = df["trade_time"].dt.minute

df.to_parquet(OUTPUT_FILE, index=False)

print("Conversion complete.")
print(f"Rows converted: {len(df)}")
print(f"Columns: {list(df.columns)}")
print(f"Saved to: {OUTPUT_FILE}")
