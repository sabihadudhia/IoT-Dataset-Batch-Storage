# IoT Dataset Batch Storage

## Project Overview
This project implements a data engineering pipeline that ingests environmental sensor telemetry data from IoT devices, streams it through Apache Kafka, validates it for data quality, and stores it in MongoDB. The system is fully containerized using Docker, ensuring portability across environments with no local machine dependencies. It is designed to be scalable and migratable to distributed cloud setups in the long term.

## Results
Environmental sensor readings from 3 IoT devices (405,184 records spanning 07/12/2020 – 07/19/2020) are streamed through Kafka and inserted into MongoDB with data quality validation. Valid records are stored in `sensor_readings`, invalid records are stored in `sensor_rejected`, and dangerous but valid readings are flagged with an alert field.

## Features
- Real-time sensor data simulation via a Kafka producer
- Data quality validation with range checks and null field detection
- Alert flagging for dangerous but valid readings (e.g. CO > 200ppm)
- Flexible document-oriented storage in MongoDB with no predefined schema
- Separate rejection collection for invalid records with rejection reasons
- Fully containerized using Docker and Docker Compose
- Automatic database and collection initialisation via `init.js`
- Retry logic for MongoDB and Kafka connection handling

## Technologies
- Python 3.12.6
- MongoDB (`mongo:latest`) — document-oriented database
- Apache Kafka (`confluentinc/cp-kafka:7.8.7`) — message streaming
- ZooKeeper (`confluentinc/cp-zookeeper:7.8.7`) — Kafka cluster coordination
- PyMongo — Python driver for MongoDB
- kafka-python — Python client for Kafka
- Pandas — CSV reading and data processing
- Docker & Docker Compose — containerization and orchestration

## Setup / Installation

### 1. Clone the repository:
```bash
git clone https://github.com/sabihadudhia/IoT-Dataset-Batch-Storage
cd IoT-Dataset-Batch-Storage
```

### 2. Download the dataset:
Download the IoT telemetry dataset from Kaggle:
https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k

Place the file named `iot_telemetry_data.csv` in the root of the project directory:
```
IoT-Dataset-Batch-Storage/
└── iot_telemetry_data.csv   ← place here
```

### 3. Install Docker:
Download and install Docker Desktop from https://www.docker.com/products/docker-desktop.
Ensure Docker is running before proceeding.

### 4. Build and run the containers:
```bash
docker-compose up --build
```
This will:
- Pull and start the MongoDB container
- Initialise `sensor_db` with `sensor_readings` and `sensor_rejected` collections
- Pull and start ZooKeeper and Kafka containers
- Build and run the producer container — simulates sensor data streaming to Kafka
- Build and run the consumer container — reads from Kafka, validates and inserts into MongoDB

## Usage

- **Run the full pipeline:**
```bash
docker-compose up --build
```

- **Stop the containers:**
```bash
docker-compose down
```

- **Rebuild from scratch:**
```bash
docker-compose down --volumes
docker-compose up --build
```

## Project Structure
```
├── producer.py              # Reads CSV and streams records to Kafka
├── consumer.py              # Reads from Kafka, validates and inserts into MongoDB
├── python_main.py           # Original batch ingestion script (reference)
├── init.js                  # MongoDB database and collection initialisation
├── Dockerfile.producer      # Producer container build instructions
├── Dockerfile.consumer      # Consumer container build instructions
├── docker-compose.yml       # Container orchestration
├── requirements.txt         # Python dependencies
├── .gitignore               # Excludes dataset from version control
└── README.md                # Documentation
```

## Data Quality Validation

Records are validated before insertion. Invalid records are stored in `sensor_rejected` with a `rejection_reason` field.

| Field | Valid Range |
|---|---|
| temp | -100°C to 80°C |
| humidity | 5% to 95% |
| co | 0 to 1 % ppm |
| lpg | 0 to 0.2 |
| smoke | 0 to 0.05 |

Alert thresholds for dangerous but valid readings:

| Field | Alert Threshold |
|---|---|
| co | > 0.01 % ppm  |
| smoke | > 0.04 % ppm |
| lpg | > 0.15 % ppm |
| temp | > 43°C |

## Outputs / Example

Valid document stored in `sensor_readings`:
```json
{
  "ts": 1594512094,
  "device": "b8:27:eb:bf:9d:51",
  "co": 0.004956,
  "humidity": 51.0,
  "light": false,
  "lpg": 0.007651,
  "motion": false,
  "smoke": 0.020411,
  "temp": 22.7
}
```

Alerted document stored in `sensor_readings`:
```json
{
  "ts": 1594512094,
  "device": "b8:27:eb:bf:9d:51",
  "co": 250,
  "alert": true,
  "reason": "co above alert threshold (200)"
}
```

Rejected document stored in `sensor_rejected`:
```json
{
  "ts": 1594512094,
  "device": "b8:27:eb:bf:9d:51",
  "temp": 999,
  "rejection_reason": "temp out of range [-100, 80]"
}
```

## Notes / Additional Info
- The dataset CSV file is excluded from the repository via `.gitignore` due to GitHub's 100MB file size limit. It must be downloaded manually from Kaggle and placed in the project root before running.
- MongoDB credentials are defined in `docker-compose.yml` (default: `admin` / `password`). These are suitable for local development only.
- All containers communicate via Docker's bridge network — service hostnames (`mongodb`, `kafka`, `zookeeper`) are used instead of `localhost`.
- Kafka and ZooKeeper may take 10-20 seconds to initialise. Retry logic in both `producer.py` and `consumer.py` handles this automatically with up to 5 attempts.
- The producer simulates real-time sensor data with a small delay between messages. Reduce `time.sleep()` in `producer.py` for faster testing.
