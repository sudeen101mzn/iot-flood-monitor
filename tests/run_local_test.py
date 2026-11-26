"""Local end-to-end test of part D (no ThingsBoard needed).

Starts a local TLS MQTT broker (amqtt), a subscriber that records every telemetry payload,
and the runner with a scripted outage. Then checks: no data lost, replay in chronological
order with original timestamps, outage + reconnect visible, and authentication failure
handled differently from network failure.

Run:  python tests/run_local_test.py
"""
import csv
import json
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt

ROOT = Path(__file__).resolve().parent.parent
CERTS = ROOT / "certs"
NODES = {"WQ-BID-001": 27.9115, "WQ-BID-018": 27.9072}     # node -> latitude from demo_scenario.json (key for matching)


def wait_port(port, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), 1).close()
            return True
        except OSError:
            time.sleep(0.3)
    return False


def start_broker(cfg):
    exe = shutil.which("amqtt")
    if not exe:
        sys.exit("amqtt not found: pip install amqtt")
    proc = subprocess.Popen([exe, "-c", str(CERTS / cfg)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_port(8883):
        proc.kill()
        sys.exit("broker did not start")
    return proc


def run_runner(tmp, duration, script=None, extra=()):
    cmd = [sys.executable, str(ROOT / "run_demo.py"), "--nodes", ",".join(NODES), "--host", "localhost",
           "--ca-file", str(CERTS / "cert.pem"), "--autostart", "--no-stdin", "--duration", str(duration),
           "--tick", "0.5", "--db", str(tmp / "buf.db"), "--log-dir", str(tmp / "logs"),
           "--backoff-min", "0.5", "--backoff-max", "4", "--batch-size", "20", *extra]
    if script:
        cmd += ["--script", str(ROOT / script)]
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=duration + 60)
    return out.stdout + out.stderr


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_outage_replay():
    print("\n== TEST 1: outage -> buffering -> ordered batch replay ==")
    tmp = Path(tempfile.mkdtemp())
    broker = start_broker("broker_open.yaml")
    got = []
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="collector")
    sub.tls_set(ca_certs=str(CERTS / "cert.pem"))
    sub.on_message = lambda c, u, m: got.append((time.time(), json.loads(m.payload)))
    sub.on_connect = lambda c, u, f, rc, p: c.subscribe("v1/devices/me/telemetry", qos=1)
    sub.connect("localhost", 8883)
    sub.loop_start()
    time.sleep(1)
    try:
        log = run_runner(tmp, 30, "scenarios/local_test.txt")
    finally:
        time.sleep(1)
        sub.loop_stop(); sub.disconnect(); broker.terminate(); broker.wait(timeout=10)
    gen = read_csv(tmp / "logs" / "generated.csv")
    events = read_csv(tmp / "logs" / "events.csv")
    flush = read_csv(tmp / "logs" / "flush.csv")
    # collect delivered (latitude, ts) keys and per-node replay order
    delivered, replay_ts = set(), {}
    for _, p in got:
        if isinstance(p, list):
            for item in p:
                k = (item["values"]["latitude"], item["ts"])
                delivered.add(k)
                replay_ts.setdefault(item["values"]["latitude"], []).append(item["ts"])
        else:
            delivered.add((p["latitude"], p["gen_ts_ms"]))
    generated = {(NODES[r["node"]], int(r["ts_ms"])) for r in gen}
    lost = generated - delivered
    in_order = all(ts == sorted(ts) for ts in replay_ts.values())
    names = [e["event"] for e in events]
    live_msgs = sum(1 for _, p in got if not isinstance(p, list))
    replay_msgs = sum(1 for _, p in got if isinstance(p, list))
    print(f"generated={len(generated)} delivered={len(generated & delivered)} lost={len(lost)} "
          f"(ratio {len(generated & delivered) / len(generated):.1%}); live msgs={live_msgs}, replay batches={replay_msgs}")
    print("outage on/off logged:", "OUTAGE_ON" in names, "OUTAGE_OFF" in names,
          "| NETWORK_FAILURE events:", names.count("NETWORK_FAILURE"))
    for r in flush:
        print("flush:", r)
    ok = (not lost) and in_order and "OUTAGE_ON" in names and "OUTAGE_OFF" in names \
        and names.count("NETWORK_FAILURE") >= 2 and len(flush) >= 1 and replay_msgs >= 1
    print("replay batches in chronological order per node:", in_order)
    print("TEST 1:", "PASS" if ok else "FAIL")
    if not ok:
        print(log[-3000:])
    return ok


def test_auth_failure():
    print("\n== TEST 2: authentication failure is not retried like a network failure ==")
    tmp = Path(tempfile.mkdtemp())
    broker = start_broker("broker_auth.yaml")
    try:
        log = run_runner(tmp, 8)
    finally:
        broker.terminate(); broker.wait(timeout=10)
    events = read_csv(tmp / "logs" / "events.csv")
    names = [e["event"] for e in events]
    auth, net = names.count("AUTH_FAILURE"), names.count("NETWORK_FAILURE")
    print(f"AUTH_FAILURE events={auth} (one per node expected: {len(NODES)}), NETWORK_FAILURE events={net}")
    ok = auth == len(NODES) and net == 0
    print("TEST 2:", "PASS" if ok else "FAIL")
    if not ok:
        print(log[-2000:])
    return ok


if __name__ == "__main__":
    if not (CERTS / "cert.pem").exists():
        subprocess.run([sys.executable, str(ROOT / "tools" / "make_test_cert.py")], check=True)
    results = [test_outage_replay(), test_auth_failure()]
    print("\nALL PASS" if all(results) else "\nSOME TESTS FAILED")
    sys.exit(0 if all(results) else 1)
