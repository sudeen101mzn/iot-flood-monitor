
import os
import ssl
import paho.mqtt.client as mqtt
import config

config.load_env()

HOST = os.environ.get("TB_HOST")
PORT = int(os.environ.get("TB_PORT", "8883"))

NODES = [
    "WQ-BID-001",
    "WQ-BID-002",
    "WQ-BID-018"
]

print("ThingsBoard MQTT Connection Test")
print("--------------------------------")

for node in NODES:
    token = config.token_for(node)

    if not token:
        print(f"{node}: TOKEN MISSING")
        continue

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv311
    )

    client.username_pw_set(token)
    client.tls_set()

    connected = False

    def on_connect(client, userdata, flags, reason_code, properties):
        global connected
        connected = not reason_code.is_failure

    client.on_connect = on_connect

    try:
        client.connect(HOST, PORT, keepalive=30)
        client.loop_start()

        import time
        for _ in range(30):
            if connected:
                break
            time.sleep(0.2)

        if connected:
            print(f"{node}: MQTT CONNECTED")
        else:
            print(f"{node}: CONNECTION NOT CONFIRMED")

    except (OSError, ssl.SSLError) as exc:
        print(f"{node}: CONNECTION FAILED ({type(exc).__name__})")

    finally:
        client.loop_stop()
        client.disconnect()
