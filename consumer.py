import json
import time

from kafka import KafkaConsumer
from pymongo import MongoClient

MONGO_URI = "mongodb://admin:password@mongodb:27017/"
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "iot-sensor-data"
MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds

# Thresholds 
INVALID_LIMITS = {
    "temp": (-100, 80),
    "humidity": (5, 95),
    "co": (0, 10000),
    "lpg": (0, 0.2),
    "smoke": (0, 0.05),
}
ALERT_LIMITS = {
    "co": 200,
    "smoke": 0.05,
    "lpg": 0.2,
    "temp": 43
}


def connect_mongo_with_retry() -> MongoClient:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            print(f"Connected to MongoDB on attempt {attempt}")
            return client
        except Exception as exc:
            print(f"MongoDB connection failed. Attempt {attempt}/{MAX_RETRIES}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    raise RuntimeError("Could not connect to MongoDB after retries")


def connect_kafka_consumer_with_retry() -> KafkaConsumer:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="sensor-consumer-group",
            )
            # Force metadata fetch so connection errors happen here.
            consumer.topics()
            print(f"Connected to Kafka on attempt {attempt}")
            return consumer
        except Exception as exc:
            print(f"Kafka connection failed. Attempt {attempt}/{MAX_RETRIES}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    raise RuntimeError("Could not connect to Kafka after retries")


def is_invalid(record: dict) -> tuple[bool, str]:
    required_fields = ["temp", "humidity", "co", "light", "lpg", "smoke", "motion"]
    for field in required_fields:
        if field not in record:
            return True, f"Missing field: {field}"

    for field, (min_value, max_value) in INVALID_LIMITS.items():
        value = record.get(field)
        if value is None:
            return True, f"Null field: {field}"
        if value < min_value or value > max_value:
            return True, f"{field} out of range [{min_value}, {max_value}]"

    motion_value = record.get("motion")
    if motion_value not in (True, False):
        return True, "motion must be True or False"

    return False, ""


def is_alert(record: dict) -> tuple[bool, str]:
    for field, threshold in ALERT_LIMITS.items():
        value = record.get(field)
        if value is not None and value > threshold:
            return True, f"{field} above alert threshold ({threshold})"

    return False, ""


def main() -> None:
    mongo_client = connect_mongo_with_retry()
    db = mongo_client["sensor_db"]
    sensor_readings = db["sensor_readings"]
    sensor_rejected = db["sensor_rejected"]

    consumer = connect_kafka_consumer_with_retry()
    print(f"Subscribed to topic: {KAFKA_TOPIC}")

    try:
        for message in consumer:
            record = message.value

            invalid, invalid_reason = is_invalid(record)
            if invalid:
                record["rejection_reason"] = invalid_reason
                sensor_rejected.insert_one(record)
                continue

            alert, alert_reason = is_alert(record)
            if alert:
                record["alert"] = True
                record["reason"] = alert_reason
                sensor_readings.insert_one(record)
            else:
                sensor_readings.insert_one(record)
    finally:
        consumer.close()
        mongo_client.close()


if __name__ == "__main__":
    main()
