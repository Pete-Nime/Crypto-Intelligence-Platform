
### ============================================================
### Project: Crypto Intelligence Platform
### File: src/build_gold_metrics.py
#
### Purpose:
### Builds business-level crypto metrics from the Silver Layer.
#
### Pipeline:
### Silver Parquet Layer → Gold Crypto Metrics
### ============================================================

import pandas as pd


df = pd.read_parquet("data/silver/crypto_trades")


gold = (
    df.groupby("symbol")
      .agg(
          total_trades=("trade_id", "count"),
          total_volume=("quantity", "sum"),
          total_trade_value=("trade_value", "sum"),
          average_price=("price", "mean"),
          max_price=("price", "max"),
          min_price=("price", "min")
      )
      .reset_index()
      .sort_values("total_trade_value", ascending=False)
)


gold.to_parquet(
    "data/gold_crypto_metrics.parquet",
    index=False
)


print("Gold crypto metrics created.")
print(gold)
