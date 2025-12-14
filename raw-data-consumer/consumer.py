from kafka import KafkaConsumer
import json
import sys

consumer = KafkaConsumer(
    'iot-sensor-topic',
    'weather-api-topic',
    bootstrap_servers='kafka:9092',
    group_id='iot-debug-v1',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Kafka consumer started...", flush=True)

for msg in consumer:
    data = msg.value
    print("RAW:", data, flush=True)


    payload = json.loads(data["payload"])
    print("Parsed payload:", payload, flush=True)