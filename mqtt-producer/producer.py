import json
import time
import random
import paho.mqtt.client as mqtt

BROKER = "emqx"
TOPIC = "esp32/temperature"

client = mqtt.Client(client_id="iot_py_device")
client.connect(BROKER, 1883, 60)

while True:
    data = {
        "device_id": "py_device_01",
        "temperature": round(random.uniform(26, 34), 2),
        "humidity": round(random.uniform(45, 70), 2),
        "ts": int(time.time())
    }

    client.publish(TOPIC, json.dumps(data))
    print("MQTT sent:", data)
    time.sleep(2)
