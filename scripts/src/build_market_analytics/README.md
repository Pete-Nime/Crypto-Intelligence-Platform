### ============================================================
### Project: Crypto Intelligence Platform
### File: src/build_market_analytics.py
#
### Purpose:
### Builds market-level analytics from the Silver Layer.
#
### Pipeline:
### Silver Parquet Layer → Gold Market Analytics
### ============================================================

import pandas as pd


df = pd.read_parquet("data/silver/crypto_trades")


market = (
    df.groupby("symbol")
      .agg(
          total_trades=("trade_id", "count"),
          total_volume=("quantity", "sum"),
          total_trade_value=("trade_value", "sum"),
          average_price=("price", "mean"),
          highest_price=("price", "max"),
          lowest_price=("price", "min"),
          largest_trade=("trade_value", "max")
      )
      .reset_index()
)


market["price_spread"] = (
    market["highest_price"] - market["lowest_price"]
)


market["trade_value_rank"] = (
    market["total_trade_value"]
    .rank(ascending=False, method="dense")
    .astype(int)
)


market["trade_count_rank"] = (
    market["total_trades"]
    .rank(ascending=False, method="dense")
    .astype(int)
)


market.to_parquet(
    "data/gold_market_analytics.parquet",
    index=False
)


print("Gold market analytics created.")
print(market.sort_values("trade_value_rank"))
