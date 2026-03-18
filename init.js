db = db.getSiblingDB('sensor_db');
db.createCollection('sensor_readings');
db.sensor_readings.createIndex({ ts: 1 })

db.createCollection('sensor_rejected');
db.sensor_rejected.createIndex({ ts: 1 })
db.sensor_rejected.createIndex({ device: 1 })
