import json
import ssl
import paho.mqtt.client as mqtt

# =======================
# KONFIGURASI MQTT
# =======================
BROKER = ""
PORT = 
TOPIC = ""

USERNAME = ""
PASSWORD = ""

CLIENT_ID = ""

# =======================
# CALLBACK FUNCTIONS
# =======================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker")
        client.subscribe(TOPIC)
        print(f"Subscribed to topic: {TOPIC}")
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    print("\n===== DATA DITERIMA =====")
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        print(f"Suhu              : {data.get('suhu')} °C")
        print(f"Kelembapan Udara  : {data.get('kelembapan_udara')} %")
        print(f"Kelembapan Tanah  : {data.get('kelembapan_tanah')} %")
        print(f"Cahaya            : {data.get('cahaya')} lux")
        print(f"Status            : {data.get('status')}")
        print(f"Durasi Siram (Z)  : {data.get('durasi_siram')}")

    except Exception as e:
        print("Error parsing message:", e)
        print("Raw payload:", msg.payload)

    print("========================")

# =======================
# MQTT CLIENT SETUP
# =======================
client = mqtt.Client(client_id=CLIENT_ID)

client.username_pw_set(USERNAME, PASSWORD)

# TLS (sesuai espClient.setInsecure())
client.tls_set(
    cert_reqs=ssl.CERT_NONE,
    tls_version=ssl.PROTOCOL_TLS
)
client.tls_insecure_set(True)

client.on_connect = on_connect
client.on_message = on_message

# =======================
# CONNECT & LOOP
# =======================
client.connect(BROKER, PORT, keepalive=60)

print("Waiting for messages...")
client.loop_forever()
