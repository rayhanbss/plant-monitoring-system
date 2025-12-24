from confluent_kafka import Consumer
from cassandra.cluster import Cluster
import json
import os
import time
from datetime import datetime

# Cassandra configuration
print("Connecting to Cassandra...", flush=True)
cluster = Cluster(['cassandra'])
session = cluster.connect()

# Run Schema Migrations
# print("Running schema migrations...", flush=True)
# with open('schema.cql', 'r') as f:
#     commands = f.read().split(';')
#     for command in commands:
#         command = command.strip()
#         if command:
#             try:
#                 session.execute(command)
#                 print(f"✓ Executed: {command[:60]}...", flush=True)
#             except Exception as e:
#                 print(f"Warning: {e}", flush=True)

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

# Kafka Consumer configuration
consumer = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'group.id': 'sensor-data-v100',
    'auto.offset.reset': 'earliest',
    'broker.address.family': 'v4',
    'client.id': 'public-consumer-1',
})

consumer.subscribe(['mqtt-data', 'weather-api-topic'])

print("Confluent Kafka consumer started. Waiting for messages...", flush=True)

while True:
    msg = consumer.poll(timeout=1.0)

    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}", flush=True)
        continue
    
    try:
        data = json.loads(msg.value().decode('utf-8'))
        topic = msg.topic()
        
        print(f"[{topic}] Received: {data}", flush=True)
        
        if topic == 'mqtt-data':
            # Parse sensor data - data is already at top level
            device_id = data.get('clientid', 'esp32')
            
            # Handle timestamp from data (in milliseconds) or use current time
            timestamp_value = data.get('timestamp')
            if timestamp_value:
                # Convert from milliseconds to datetime
                timestamp = datetime.fromtimestamp(timestamp_value / 1000)
            else:
                timestamp = datetime.utcnow()
            
            temperature = data.get('suhu')
            humidity = data.get('kelembapan_udara')
            soil_moisture = data.get('kelembapan_tanah')
            light_level = data.get('cahaya')
            status = data.get('status')
            duration = data.get('durasi_siram')
            
            # Insert into Cassandra
            session.execute(insert_sensor, (
                device_id, timestamp, temperature, humidity, soil_moisture, light_level, status, duration
            ))
            print(f"✓ Inserted sensor data for device: {device_id}", flush=True)
            
        elif topic == 'weather-api-topic':
            # Parse weather data
            payload = json.loads(data.get("payload", "{}")) if isinstance(data.get("payload"), str) else data.get("payload", {})
            
            city_id = payload.get('city_id', 'unknown')
            timestamp_str = payload.get('timestamp')
            if timestamp_str:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                timestamp = datetime.utcnow()
            city_name = payload.get('city_name')
            latitude = payload.get('latitude')
            longitude = payload.get('longitude')
            temperature = payload.get('temp') 
            weather_condition = payload.get('weather_main')
            weather_description = payload.get('weather_description')
            
            # Insert into Cassandra
            session.execute(insert_weather, (
                city_id, timestamp, city_name, latitude, longitude, temperature, weather_condition, weather_description
            ))
            print(f"✓ Inserted weather data for location: {city_name}", flush=True)
            
    except Exception as e:
        print(f"✗ Error processing message: {e}", flush=True)
        print(f"  Message was: {msg.value()}", flush=True)