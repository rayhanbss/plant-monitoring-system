from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import sys
import time
from datetime import datetime

print("Connecting to Cassandra...", flush=True)
cluster = Cluster(['cassandra'])
session = cluster.connect()

# Run Schema Migrations
print("Running schema migrations...", flush=True)
with open('schema.cql', 'r') as f:
    commands = f.read().split(';')
    for command in commands:
        command = command.strip()
        if command:
            try:
                session.execute(command)
                print(f"✓ Executed: {command[:60]}...", flush=True)
            except Exception as e:
                print(f"Warning: {e}", flush=True)

# Use the keyspace
session.set_keyspace('plant_monitoring')
print("Schema initialized successfully", flush=True)

# Prepare insert statements
insert_sensor = session.prepare("""
    INSERT INTO sensor_data (device_id, timestamp, temperature, humidity, soil_moisture, light_level, status, duration)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

insert_weather = session.prepare("""
    INSERT INTO weather_data (city_id, timestamp, city_name, latitude, longitude, temperature, weather_condition, weather_description)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

# Wait for Kafka to be ready
print("Waiting for Kafka...", flush=True)
time.sleep(5)

consumer = KafkaConsumer(
    'iot-sensor-topic',
    'weather-api-topic',
    bootstrap_servers='kafka:9092',
    group_id='plant-monitoring-consumer',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Kafka consumer started. Waiting for messages...", flush=True)

for msg in consumer:
    try:
        data = msg.value
        topic = msg.topic
        
        print(f"[{topic}] Received: {data}", flush=True)
        
        if topic == 'iot-sensor-topic':
            # Parse sensor data
            payload = json.loads(data.get("payload", "{}")) if isinstance(data.get("payload"), str) else data.get("payload", {})
            
            device_id = payload.get('device_id', 'unknown')
            timestamp = datetime.utcnow()
            temperature = payload.get('suhu')
            humidity = payload.get('kelembapan_udara')
            soil_moisture = payload.get('kelembapan_tanah')
            light_level = payload.get('cahaya')
            status = payload.get('status')
            duration = payload.get('durasi_siram')
            
            # Insert into Cassandra
            session.execute(insert_sensor, (
                device_id, timestamp, temperature, humidity, soil_moisture, light_level, status, duration
            ))
            print(f"✓ Inserted sensor data for device: {device_id}", flush=True)
            
        elif topic == 'weather-api-topic':
            # Parse weather data
            payload = json.loads(data.get("payload", "{}")) if isinstance(data.get("payload"), str) else data.get("payload", {})
            
            city_id = payload.get('city_id', 'unknown')
            # Parse timestamp string to datetime object
            timestamp_str = payload.get('timestamp')
            if timestamp_str:
                # Remove 'Z' and parse ISO format
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                timestamp = datetime.utcnow()
            city_name = payload.get('city_name')
            latitude = payload.get('latitude')
            longitude = payload.get('longitude')
            temperature = payload.get('temp')  # API uses 'temp' not 'temperature'
            weather_condition = payload.get('weather_main')  # API uses 'weather_main'
            weather_description = payload.get('weather_desc')  # API uses 'weather_desc'
            
            # Insert into Cassandra
            session.execute(insert_weather, (
                city_id, timestamp, city_name, latitude, longitude, temperature, weather_condition, weather_description
            ))
            print(f"✓ Inserted weather data for location: {city_name}", flush=True)
            
    except Exception as e:
        print(f"✗ Error processing message: {e}", flush=True)
        print(f"  Message was: {msg.value}", flush=True)