import json
import time
from datetime import datetime

from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import PyMongoError, DuplicateKeyError

MONGO_URI = "mongodb://admin:password@mongodb:27017/"
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "iot-sensor-data"
MAX_RETRIES = 10
RETRY_DELAY = 10

INVALID_LIMITS = {
    "temp": (-100, 80),
    "humidity": (5, 95),
    "co": (0, 1),
    "lpg": (0, 0.2),
    "smoke": (0, 0.05),
}
ALERT_LIMITS = {
    "co": 0.01,
    "smoke": 0.04,
    "lpg": 0.15,
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
            consumer.topics()
            print(f"Connected to Kafka on attempt {attempt}")
            return consumer
        except Exception as exc:
            print(f"Kafka connection failed. Attempt {attempt}/{MAX_RETRIES}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    raise RuntimeError("Could not connect to Kafka after retries")


def load_active_alerts(sensor_alerts) -> dict:
    """Load existing active alerts from MongoDB on startup."""
    active = {}
    for alert in sensor_alerts.find({"active": True}):
        key = (alert["device"], alert["field"])
        active[key] = alert
    print(f"Loaded {len(active)} active alerts from MongoDB")
    return active


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


def process_alerts(record: dict, active_alerts: dict, sensor_alerts, sensor_readings) -> None:
    """
    Stateful alert processing:
    - Fire once when threshold first crossed
    - Suppress while alert is already active
    - Clear when reading returns to normal
    """
    device = record.get("device")
    ts = record.get("ts")

    for field, threshold in ALERT_LIMITS.items():
        value = record.get(field)
        if value is None:
            continue

        key = (device, field)
        alert_active = key in active_alerts

        if value > threshold:
            if not alert_active:
                # Fire alert — first time threshold crossed
                alert_doc = {
                    "device": device,
                    "field": field,
                    "threshold": threshold,
                    "value": value,
                    "started_at": ts,
                    "cleared_at": None,
                    "acknowledged": False,
                    "active": True
                }
                try:
                    sensor_alerts.insert_one(alert_doc)
                    active_alerts[key] = alert_doc
                    print(f"ALERT FIRED: {device} - {field} = {value} > {threshold}")
                except PyMongoError as e:
                    print(f"Failed to insert alert: {e}")
            # If alert already active — suppress, do nothing extra

        else:
            if alert_active:
                # Clear alert — reading returned to normal
                try:
                    sensor_alerts.update_one(
                        {"device": device, "field": field, "active": True},
                        {"$set": {"active": False, "cleared_at": ts}}
                    )
                    del active_alerts[key]
                    print(f"ALERT CLEARED: {device} - {field} returned to normal")
                except PyMongoError as e:
                    print(f"Failed to clear alert: {e}")

    # Store reading normally
    try:
        sensor_readings.insert_one(record)
    except DuplicateKeyError:
        pass
    except PyMongoError as e:
        print(f"Insert failed: {e}")


def main() -> None:
    print("Waiting for Kafka and MongoDB to be ready...")
    time.sleep(20)

    mongo_client = connect_mongo_with_retry()
    db = mongo_client["sensor_db"]
    sensor_readings = db["sensor_readings"]
    sensor_rejected = db["sensor_rejected"]
    sensor_alerts = db["sensor_alerts"]

    # Load active alerts from MongoDB — survives restarts
    active_alerts = load_active_alerts(sensor_alerts)

    consumer = connect_kafka_consumer_with_retry()
    print(f"Subscribed to topic: {KAFKA_TOPIC}")

    try:
        for message in consumer:
            record = message.value

            invalid, invalid_reason = is_invalid(record)
            if invalid:
                record["rejection_reason"] = invalid_reason
                try:
                    sensor_rejected.insert_one(record)
                except DuplicateKeyError:
                    pass
                except PyMongoError as e:
                    print(f"Rejected insert failed: {e}")
                continue

            process_alerts(record, active_alerts, sensor_alerts, sensor_readings)

    finally:
        consumer.close()
        mongo_client.close()


if __name__ == "__main__":
    main()
