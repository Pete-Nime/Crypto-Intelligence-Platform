### ============================================================
### Project: Crypto Intelligence Platform
### File: src/spark_to_parquet.py
#
### Purpose:
### Reads live crypto trades from Kafka using Spark Structured
### Streaming and writes clean data into Parquet files.
#
### Pipeline:
### Kafka Topic → Spark Streaming → Silver Parquet Layer
### ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
    hour,
    minute
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType
)


spark = (
    SparkSession.builder
    .appName("CryptoSparkToParquet")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


trade_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("trade_id", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("trade_value", DoubleType(), True),
    StructField("trade_time", StringType(), True),
    StructField("ingestion_time", StringType(), True)
])


raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "crypto-trades")
    .option("startingOffsets", "latest")
    .load()
)


silver_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_string")
    .select(from_json(col("json_string"), trade_schema).alias("data"))
    .select("data.*")
    .withColumn("trade_timestamp", to_timestamp(col("trade_time")))
    .withColumn("ingestion_timestamp", to_timestamp(col("ingestion_time")))
    .withColumn("trade_date", to_date(col("trade_timestamp")))
    .withColumn("trade_hour", hour(col("trade_timestamp")))
    .withColumn("trade_minute", minute(col("trade_timestamp")))
)


query = (
    silver_stream.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/silver/crypto_trades")
    .option("checkpointLocation", "data/checkpoints/crypto_trades")
    .start()
)


print("Spark Silver Layer streaming started.")
print("Writing clean Parquet files to: data/silver/crypto_trades")
print("Press CTRL + C to stop.")

query.awaitTermination()
