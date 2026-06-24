### ============================================================
### Project: Crypto Intelligence Platform
### File: src/build_time_analytics.py
#
### Purpose:
### Builds time-based analytics from the Silver Layer.
#
### Pipeline:
### Silver Parquet Layer → Gold Time Metrics
### ============================================================

import pandas as pd


df = pd.read_parquet("data/silver/crypto_trades")


time_metrics = (
    df.groupby(["symbol", "trade_date", "trade_hour", "trade_minute"])
      .agg(
          total_trades=("trade_id", "count"),
          total_volume=("quantity", "sum"),
          total_trade_value=("trade_value", "sum"),
          average_price=("price", "mean"),
          max_price=("price", "max"),
          min_price=("price", "min")
      )
      .reset_index()
      .sort_values(["trade_date", "trade_hour", "trade_minute", "symbol"])
)


time_metrics.to_parquet(
    "data/gold_time_metrics.parquet",
    index=False
)


print("Gold time analytics created.")
print(time_metrics.head())
print("Rows:", len(time_metrics))
