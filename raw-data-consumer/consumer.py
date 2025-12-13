from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'sensor_iot',
    bootstrap_servers='kafka:9092',
    group_id='iot-debug-v1',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Kafka consumer started...")

for msg in consumer:
    data = msg.value
    print("RAW:", data)

    # parse payload JSON string
    payload = json.loads(data["payload"])

    print("Parsed payload:", payload)
    print("Temperature:", payload["temperature"])
