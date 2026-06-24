
### ============================================================
# Project: Crypto Intelligence Platform
### File: src/binance_kafka_producer.py
#
# Purpose:
### Connects to Binance WebSocket, receives live crypto trades,
### cleans the data, and sends it into Kafka.
#
### Pipeline:
### Binance WebSocket → Python Producer → Kafka Topic
### ============================================================

import json
import websocket
from kafka import KafkaProducer
from datetime import datetime


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "crypto-trades"

SYMBOLS = [
    "btcusdt",
    "ethusdt",
    "solusdt",
    "bnbusdt"
]

STREAMS = [f"{symbol}@trade" for symbol in SYMBOLS]

BINANCE_SOCKET = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(STREAMS)
)


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def on_message(ws, message):
    try:
        raw_message = json.loads(message)
        data = raw_message["data"]

        trade = {
            "event_type": data["e"],
            "symbol": data["s"],
            "trade_id": data["t"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "trade_value": float(data["p"]) * float(data["q"]),
            "trade_time": datetime.fromtimestamp(data["T"] / 1000).isoformat(),
            "ingestion_time": datetime.now().isoformat()
        }

        producer.send(KAFKA_TOPIC, trade)
        producer.flush()

        print(f"Sent to Kafka: {trade}")

    except Exception as error:
        print(f"Error processing message: {error}")


def on_open(ws):
    print("Connected to Binance WebSocket")
    print(f"Streaming symbols: {', '.join([s.upper() for s in SYMBOLS])}")
    print(f"Sending data to Kafka topic: {KAFKA_TOPIC}")


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")
    print(f"Close status code: {close_status_code}")
    print(f"Close message: {close_msg}")


if __name__ == "__main__":
    print("Starting Crypto Intelligence Platform producer...")
    print(f"Connecting to: {BINANCE_SOCKET}")

    ws = websocket.WebSocketApp(
        BINANCE_SOCKET,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever(sslopt={"cert_reqs": 0})
