import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "iot-sensor-data"
MAX_RETRIES = 10
RETRY_DELAY = 10  # seconds


def create_producer_with_retry() -> KafkaProducer:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            print(f"Connected to Kafka on attempt {attempt}")
            return producer
        except Exception as exc:
            print(f"Kafka connection failed. Attempt {attempt}/{MAX_RETRIES}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    raise RuntimeError("Could not connect to Kafka after retries")


def main() -> None:
    print("Waiting for Kafka and MongoDB to be ready...")
    time.sleep(20)
    
    producer = create_producer_with_retry()

    csv_path = Path(__file__).with_name("iot_telemetry_data.csv")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    for _, row in df.iterrows():
        message = row.to_dict()
        producer.send(KAFKA_TOPIC, message)
        time.sleep(0.001)  

    producer.flush()
    producer.close()
    print("Finished sending data to Kafka")


if __name__ == "__main__":
    main()
