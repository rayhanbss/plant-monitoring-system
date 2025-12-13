import json
import time
import random
import paho.mqtt.client as mqtt

BROKER = "emqx"
TOPIC = "esp32/temperature"

def on_connect(client, userdata, flags, reason_code, properties=None):
    print("Connected to EMQX with code:", reason_code)

client = mqtt.Client(
    client_id="iot_py_device",
    protocol=mqtt.MQTTv311,
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
)

client.on_connect = on_connect

# retry loop (WAJIB di docker)
while True:
    try:
        client.connect(BROKER, 1883, 60)
        break
    except Exception as e:
        print("Waiting EMQX...", e)
        time.sleep(3)

client.loop_start()

while True:
    payload = {
        "device_id": "esp32_01",
        "temperature": round(random.uniform(25, 35), 2),
        "humidity": round(random.uniform(40, 70), 2)
    }

    client.publish(TOPIC, json.dumps(payload))
    print("MQTT sent:", payload)
    time.sleep(3)