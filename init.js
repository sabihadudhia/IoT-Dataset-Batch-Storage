db = db.getSiblingDB('sensor_db');
db.createCollection('sensor_readings');
db.sensor_readings.createIndex({ ts: 1 })

