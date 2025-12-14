import json
import ssl
import time
import os
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from kafka import KafkaProducer

# KONFIGURASI MQTT
BROKER = os.getenv("MQTT_BROKER")
PORT = int(os.getenv("MQTT_PORT"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")
USERNAME = os.getenv("MQTT_USERNAME")
PASSWORD = os.getenv("MQTT_PASSWORD")

CLIENT_ID = os.getenv("MQTT_CLIENT_ID")

# KONFIGURASI KAFKA
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# KAFKA PRODUCER INITIALIZATION
kafka_producer = None

def init_kafka():
    global kafka_producer
    while True:
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print("✓ Connected to Kafka")
            break
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(3)

# CALLBACK FUNCTIONS
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("✓ Connected to MQTT Broker")
        client.subscribe(MQTT_TOPIC)
        print(f"✓ Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"✗ Failed to connect, return code {reason_code}")

def on_message(client, userdata, msg):
    print("\n===== DATA RECEIVED =====")
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        print(f"Suhu              : {data.get('suhu')} °C")
        print(f"Kelembapan Udara  : {data.get('kelembapan_udara')} %")
        print(f"Kelembapan Tanah  : {data.get('kelembapan_tanah')} %")
        print(f"Cahaya            : {data.get('cahaya')} lux")
        print(f"Status            : {data.get('status')}")
        print(f"Durasi Siram (Z)  : {data.get('durasi_siram')}")

        # Publish to Kafka
        kafka_message = {
            "source": "esp32",
            "topic": MQTT_TOPIC,
            "payload": json.dumps(data),
            "timestamp": time.time()
        }
        
        kafka_producer.send(KAFKA_TOPIC, kafka_message)
        kafka_producer.flush()
        print(f"→ Published to Kafka topic: {KAFKA_TOPIC}")

    except Exception as e:
        print("✗ Error processing message:", e)
        print("Raw payload:", msg.payload)

    print("=========================\n")

# MQTT CLIENT SETUP
init_kafka()

client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id=CLIENT_ID)

client.username_pw_set(USERNAME, PASSWORD)

# TLS (sesuai espClient.setInsecure())
client.tls_set(
    cert_reqs=ssl.CERT_NONE,
    tls_version=ssl.PROTOCOL_TLS
)
client.tls_insecure_set(True)

client.on_connect = on_connect
client.on_message = on_message

# CONNECT & LOOP
print("Starting MQTT-to-Kafka Bridge...")
client.connect(BROKER, PORT, keepalive=60)

print("Listening for MQTT messages and forwarding to Kafka...")
client.loop_forever()