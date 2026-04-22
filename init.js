db = db.getSiblingDB('sensor_db');
db.createCollection('sensor_readings');
db.sensor_readings.createIndex({ ts: 1 })
db.sensor_readings.createIndex({ device: 1, ts: 1 }, { unique: true })

db.createCollection('sensor_rejected');
db.sensor_rejected.createIndex({ ts: 1 })
db.sensor_rejected.createIndex({ device: 1 })

db.createCollection('sensor_alerts');
db.sensor_alerts.createIndex({ device: 1, field: 1, active: 1 })

