### ============================================================
### Project: Crypto Intelligence Platform
### File: src/kafka_trade_consumer.py
#
### Purpose:
### Reads live crypto trade messages from Kafka and saves them
### into a JSON Lines file.
#
### Pipeline:
### Kafka Topic → Python Consumer → JSONL Storage
### ============================================================

import json
from kafka import KafkaConsumer
from datetime import datetime
from pathlib import Path


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "crypto-trades"

OUTPUT_FILE = Path("data/crypto_trades.jsonl")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="crypto-trade-consumer-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)


print("Kafka consumer started...")
print(f"Reading from topic: {KAFKA_TOPIC}")
print(f"Saving data to: {OUTPUT_FILE}")
print("Press CTRL + C to stop.\n")


try:
    for message in consumer:
        trade = message.value
        trade["consumer_received_time"] = datetime.now().isoformat()

        with open(OUTPUT_FILE, "a") as file:
            file.write(json.dumps(trade) + "\n")

        print("Saved trade:", trade)

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

finally:
    consumer.close()
    print("Kafka consumer closed.")
