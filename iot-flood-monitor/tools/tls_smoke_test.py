"""First TLS smoke test for ThingsBoard (part D).

Publishes two messages over MQTT/TLS (port 8883, QoS 1):
  1. a live message (no timestamp): ThingsBoard stamps it on arrival
  2. a historic message with an explicit epoch-millisecond timestamp
     (10 minutes ago): it must appear on the chart 10 minutes in the past

It also tells authentication failures apart from network failures.
Set the token first (PowerShell):  $env:TB_TOKEN = "your-device-token"
"""
import json
import os
import socket
import ssl
import sys
import threading
import time

import paho.mqtt.client as mqtt

HOST = os.getenv("TB_HOST", "demo.thingsboard.io")
PORT = 8883
TOKEN = os.getenv("TB_TOKEN")
TOPIC = "v1/devices/me/telemetry"


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def main():
    if not TOKEN:
        sys.exit("Set TB_TOKEN first, for example: $env:TB_TOKEN = 'abc123'")

    connack = {}
    connected = threading.Event()

    def on_connect(client, userdata, flags, reason_code, properties):
        connack["rc"] = reason_code
        connected.set()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="", protocol=mqtt.MQTTv311)
    client.username_pw_set(TOKEN)          # ThingsBoard: token is the username, no password
    client.tls_set()                       # trust the operating system's CA certificates
    client.on_connect = on_connect

    # Step 1: open the TCP + TLS connection (failures here are NETWORK or TLS problems)
    try:
        client.connect(HOST, PORT, keepalive=30)
    except ssl.SSLError as exc:
        sys.exit(f"[TLS FAILURE] certificate or handshake problem: {exc}")
    except socket.gaierror as exc:
        sys.exit(f"[NETWORK FAILURE] cannot resolve {HOST}: {exc}")
    except (ConnectionRefusedError, TimeoutError, OSError) as exc:
        sys.exit(f"[NETWORK FAILURE] cannot reach {HOST}:{PORT}: {exc}")

    client.loop_start()

    # Step 2: wait for the broker's answer (a refusal here is an AUTHENTICATION problem)
    if not connected.wait(timeout=10):
        client.loop_stop()
        sys.exit("[NETWORK FAILURE] TLS connected but no CONNACK within 10 s")
    rc = connack["rc"]
    if rc.is_failure:
        client.loop_stop()
        # paho 2.x reports the MQTT 3.1.1 refusals 4/5 as the MQTT 5 codes 134/135
        kind = "AUTHENTICATION FAILURE" if rc.value in (4, 5, 134, 135) else "CONNECT REFUSED"
        sys.exit(f"[{kind}] broker answered: {rc}")
    log("connected over TLS and authenticated")

    # Step 3: publish with QoS 1 and wait for the acknowledgement (PUBACK)
    now_ms = int(time.time() * 1000)
    live = {"ph": 7.2, "turbidity_ntu": 4.1}                         # server-stamped
    historic = {"ts": now_ms - 10 * 60 * 1000,                        # client-stamped
                "values": {"ph": 6.9, "turbidity_ntu": 120.0}}
    for name, payload in (("live", live), ("historic", historic)):
        info = client.publish(TOPIC, json.dumps(payload), qos=1)
        info.wait_for_publish(timeout=10)
        log(f"{name} message acknowledged: {info.is_published()}")

    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
