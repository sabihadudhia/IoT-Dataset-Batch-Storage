# IoT Dataset Batch Storage

## Project Overview
This project implements a data engineering pipeline that ingests environmental sensor telemetry data from IoT devices and stores it in batches into a MongoDB database. The system is fully containerized using Docker, ensuring portability across environments with no local machine dependencies. It is designed to be scalable and migratable to distributed cloud setups in the long term.

## Results
Environmental sensor readings from 3 IoT devices (405,184 records spanning 07/12/2020 – 07/19/2020) are batch-inserted into MongoDB in 5-minute time windows (~200 documents per batch) using PyMongo's `insert_many()`.

## Features
- Time-window batch ingestion of IoT sensor telemetry data
- Flexible document-oriented storage in MongoDB with no predefined schema
- Fully containerized using Docker and Docker Compose
- Automatic database and collection initialisation via `init.js`
- Retry logic for MongoDB connection handling

## Technologies
- Python 3.12.6
- MongoDB (mongo:latest)
- PyMongo — Python driver for MongoDB
- Pandas — CSV reading and time-window filtering
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
└── iot_telemetry_data.csv  
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
- Initialise the `sensor_db` database and `sensor_readings` collection
- Build and run the Python container
- Insert all sensor data into MongoDB in 5-minute time window batches

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
├── python_main.py        # Main ingestion script
├── init.js               # MongoDB database and collection initialisation
├── Dockerfile            # Python container build instructions
├── docker-compose.yml    # Container orchestration
├── requirements.txt      # Python dependencies
├── .gitignore            # Excludes dataset from version control
└── README.md             # Documentation
```

## Outputs / Example
The pipeline prints progress to the console as batches are inserted:

```
Connected to MongoDB
Inserted 198 records for window 1594512094 - 1594512394
Inserted 203 records for window 1594512394 - 1594512694
...
```

Each document stored in MongoDB follows this structure:
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

## Notes / Additional Info
- The dataset CSV file is excluded from the repository via `.gitignore` due to GitHub's 100MB file size limit. It must be downloaded manually from Kaggle and placed in the project root before running.
- MongoDB credentials are defined in `docker-compose.yml` (default: `admin` / `password`). These are suitable for local development only.
- The Python container uses the Docker bridge network to connect to MongoDB via the service hostname `mongodb` — not `localhost`.
- If the pipeline fails on first run, Docker may need a moment to fully initialise MongoDB. The retry logic in `python_main.py` handles this automatically with up to 5 attempts.
