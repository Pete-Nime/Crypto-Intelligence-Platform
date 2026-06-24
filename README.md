# 🚀 Crypto Intelligence Platform

## Overview

The Crypto Intelligence Platform is a real-time data engineering project that streams live cryptocurrency trade data from Binance, processes the data using Apache Kafka and Apache Spark Structured Streaming, and generates business-ready analytics datasets.

This project demonstrates modern industry-standard data engineering techniques including:

* Real-Time Data Streaming
* Event-Driven Architecture
* Apache Kafka Messaging
* Apache Spark Structured Streaming
* Data Lake Architecture
* Bronze, Silver, and Gold Data Layers
* Analytics Engineering

The project was developed as a portfolio project to simulate how financial institutions, trading firms, and enterprise data platforms process and analyse real-time market data.

---

# Business Problem

Financial markets generate thousands of events every second.

Traditional reporting systems often process data in batches, which introduces delays.

The goal of this project is to:

* Capture live cryptocurrency trade data
* Process the data in real time
* Store analytics-ready datasets
* Generate business intelligence metrics
* Build a scalable foundation for cloud deployment

---

# Architecture

Binance WebSocket

↓

Kafka Producer

↓

Kafka Topic

↓

Spark Structured Streaming

↓

Silver Layer (Cleaned Data)

↓

Gold Layer (Business Analytics)

↓

Dashboard / Reporting Layer

---

# Technologies Used

## Python

Python is used as the primary programming language for the project.

Purpose:

* Data processing
* Kafka integration
* Spark integration
* Analytics generation

---

## Binance WebSocket API

Purpose:

Provides live cryptocurrency trade events.

Example:

BTCUSDT Trade

ETHUSDT Trade

BNBUSDT Trade

SOLUSDT Trade

This allows us to receive real-time market activity.

---

## Apache Kafka

Purpose:

Kafka acts as a message broker.

Instead of sending data directly into Spark, Kafka stores streaming events in a topic.

Benefits:

* Scalability
* Reliability
* Decoupled Architecture
* Real-Time Processing

---

## Apache Spark Structured Streaming

Purpose:

Spark consumes events from Kafka and processes them in near real time.

Responsibilities:

* Read Kafka messages
* Parse JSON data
* Apply schema validation
* Transform raw data
* Write analytics datasets

---

## Parquet

Purpose:

Store data efficiently.

Benefits:

* Fast reads
* Columnar storage
* Industry standard
* Ideal for analytics workloads

---

# Data Lake Design

## Bronze Layer

Raw Data

File:

crypto_trades.jsonl

Purpose:

Stores original trade events exactly as received.

---

## Silver Layer

Cleaned Data

Location:

data/silver/crypto_trades

Purpose:

* Schema validation
* Data enrichment
* Analytics preparation

Additional Columns:

* trade_date
* trade_hour
* trade_minute

---

## Gold Layer

Business Analytics

### gold_crypto_metrics.parquet

Contains:

* Total Trades
* Total Volume
* Average Price
* Maximum Price
* Minimum Price

---

### gold_time_metrics.parquet

Contains:

* Minute-Level Trade Volume
* Minute-Level Trade Value
* Average Price Trends

---

### gold_market_analytics.parquet

Contains:

* Trade Rankings
* Largest Trades
* Price Spread Analysis
* Market Leaderboards

---

# Project Files

src/

├── binance_kafka_producer.py

├── kafka_trade_consumer.py

├── spark_kafka_stream.py

├── build_gold_metrics.py

├── build_time_analytics.py

├── build_market_analytics.py

├── market_insights.py

└── dashboard.py

---

# Key Analytics Generated

### Total Trades

Number of completed trades.

### Total Trade Volume

Total cryptocurrency volume traded.

### Trade Value

Total monetary value traded.

### Average Price

Average market price.

### Largest Trade

Largest individual trade detected.

### Price Spread

Difference between highest and lowest prices.

### Market Ranking

Ranks cryptocurrencies by market activity.

---

# Learning Outcomes

This project demonstrates practical experience with:

* Event Streaming
* Data Engineering
* Kafka Architecture
* Spark Structured Streaming
* Data Lake Design
* Analytics Engineering
* Real-Time Data Processing
* Financial Market Analytics

---

# Future Enhancements

* AWS Deployment
* Docker Containers
* Streamlit Dashboard
* AI Generated Market Insights
* Real-Time Alerting
* Machine Learning Forecasting

---

# Author

Peter Nime

Founder

Areca Tech Limited

Building practical AI, Data Engineering, and Analytics solutions for modern businesses.
