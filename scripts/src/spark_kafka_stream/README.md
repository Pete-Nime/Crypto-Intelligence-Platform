### ============================================================
### Project: Crypto Intelligence Platform
### File: src/spark_kafka_stream.py
#
### Purpose:
### Reads live crypto trades from Kafka using Spark Structured
### Streaming and displays them in the terminal.
#
### Pipeline:
### Kafka Topic → Spark Structured Streaming → Console
### ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType
)


spark = (
    SparkSession.builder
    .appName("CryptoKafkaStreaming")
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


parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_string")
    .select(from_json(col("json_string"), trade_schema).alias("data"))
    .select("data.*")
)


query = (
    parsed_stream.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)


print("Spark Streaming Started")
print("Listening to Kafka Topic: crypto-trades")

query.awaitTermination()
