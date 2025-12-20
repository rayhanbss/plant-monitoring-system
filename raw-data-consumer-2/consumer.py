from confluent_kafka import Consumer
import json

conf = {
    'bootstrap.servers': '',
    'group.id': 'iot-debug-v1',
    'auto.offset.reset': 'earliest',

    # penting untuk ngrok
    'broker.address.family': 'v4',

    # observability
    'client.id': 'public-consumer-1'
}

consumer = Consumer(conf)
consumer.subscribe(['mqtt-data'])

print("Confluent Kafka consumer started")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        data = json.loads(msg.value().decode('utf-8', errors='ignore'))
        print("RAW:", data)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
