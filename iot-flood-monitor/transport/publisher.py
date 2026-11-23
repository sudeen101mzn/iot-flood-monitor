"""MQTT-over-TLS publisher for one ThingsBoard device (part D).

* TLS on port 8883, device access token as MQTT username (empty password).
* QoS 1: a publish only counts as delivered after the broker's PUBACK.
* Failure classification (and different handling):
    - NETWORK_FAILURE : no route, timeout, TLS problem, lost link -> retry with exponential backoff
    - AUTH_FAILURE    : broker refused the token -> retrying cannot help, so stop and say so
* Outage simulation (outage_on / outage_off): while on, every connection attempt fails exactly
  like a dead link, so the backoff, buffering and replay are exercised for real.
* Two payload forms (ThingsBoard MQTT API):
    live   : {"gen_ts_ms": ..., "ph": ...}            server stamps arrival time (used to measure latency)
    replay : [{"ts": epoch_ms, "values": {...}}, ...]  client stamps the ORIGINAL acquisition time
"""
import json
import logging
import socket
import ssl
import threading
import time
from enum import Enum

import paho.mqtt.client as mqtt

from .backoff import Backoff

TOPIC = "v1/devices/me/telemetry"
VALUE_KEYS = ("ph", "turbidity_ntu", "tds_ppm", "water_temp_c", "flow_lpm", "latitude", "longitude")
AUTH_CODES = (4, 5, 134, 135)   # paho 2.x reports MQTT 3.1.1 refusals 4/5 as 134/135


class Result(Enum):
    OK = "OK"
    NETWORK_FAILURE = "NETWORK_FAILURE"
    AUTH_FAILURE = "AUTH_FAILURE"


class TbPublisher:
    def __init__(self, node_id, token, host, port=8883, *, tls=True, ca_file=None,
                 backoff=None, connect_timeout=10.0, publish_timeout=10.0, keepalive=30,
                 recorder=None):
        self.node_id, self.token, self.host, self.port = node_id, token, host, port
        self.tls, self.ca_file = tls, ca_file
        self.backoff = backoff or Backoff()
        self.connect_timeout, self.publish_timeout, self.keepalive = connect_timeout, publish_timeout, keepalive
        self.rec = recorder
        self.log = logging.getLogger(f"transport.{node_id}")
        self.state = "DISCONNECTED"      # DISCONNECTED | CONNECTING | CONNECTED | AUTH_FAILED
        self.outage = False
        self.restored_at = None          # monotonic time of the last outage_off
        self._client = None
        self._lost = threading.Event()
        self._next_attempt = 0.0
        self._failures = 0               # consecutive failed attempts, for the log

    # ------------------------------------------------------------------ state helpers
    def _set_state(self, new, reason=""):
        if new != self.state:
            self.log.info("STATE %s -> %s %s", self.state, new, reason)
            if self.rec:
                self.rec.event(self.node_id, f"STATE_{new}", reason)
            self.state = new

    def _teardown(self):
        client, self._client = self._client, None
        if client is not None:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:           # already closed
                pass

    def _schedule_retry(self):
        delay = self.backoff.next_delay()
        self._next_attempt = time.monotonic() + delay
        return delay

    # ------------------------------------------------------------------ outage toggle
    def set_outage(self, on):
        self.outage = on
        if on:
            self.log.warning("OUTAGE ON: network deliberately cut")
            self._teardown()
            self._lost.set()
            self._set_state("DISCONNECTED", "(outage_on)")
        else:
            self.log.warning("OUTAGE OFF: network restored (next scheduled reconnect will succeed)")
            self.restored_at = time.monotonic()
        if self.rec:
            self.rec.event(self.node_id, "OUTAGE_ON" if on else "OUTAGE_OFF")

    def reset_auth(self, token=None):
        """Call after fixing the token; allows connection attempts again."""
        if token:
            self.token = token
        self.backoff.reset()
        self._next_attempt = 0.0
        self._set_state("DISCONNECTED", "(auth reset)")

    # ------------------------------------------------------------------ connecting
    def ensure_connected(self):
        if self.state == "AUTH_FAILED":
            return Result.AUTH_FAILURE
        if self.state == "CONNECTED" and self._client is not None and not self._lost.is_set():
            return Result.OK
        if time.monotonic() < self._next_attempt:
            return Result.NETWORK_FAILURE            # still waiting out the backoff delay
        return self._attempt_connect()

    def _fail_network(self, kind, exc):
        self._failures += 1
        delay = self._schedule_retry()
        self.log.warning("NETWORK FAILURE (%s): %s | attempt %d failed, retry in %.1f s",
                         kind, exc, self._failures, delay)
        if self.rec:
            self.rec.event(self.node_id, "NETWORK_FAILURE", f"{kind}; retry_in={delay:.1f}s")
        self._set_state("DISCONNECTED")
        return Result.NETWORK_FAILURE

    def _attempt_connect(self):
        self._teardown()
        self._lost.clear()
        self._set_state("CONNECTING", f"-> {self.host}:{self.port}")
        connack = {}
        got = threading.Event()

        def on_connect(client, userdata, flags, reason_code, properties):
            connack["rc"] = reason_code
            got.set()

        def on_disconnect(client, userdata, flags, reason_code, properties):
            if self._client is client:
                self._lost.set()

        try:
            if self.outage:
                raise ConnectionError("simulated outage (outage_on)")
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="", protocol=mqtt.MQTTv311)
            client.username_pw_set(self.token)
            if self.tls:
                client.tls_set(ca_certs=self.ca_file)   # None = operating-system CA store
            client.on_connect, client.on_disconnect = on_connect, on_disconnect
            client.connect(self.host, self.port, keepalive=self.keepalive)
        except ssl.SSLError as exc:
            return self._fail_network("TLS", exc)
        except socket.gaierror as exc:
            return self._fail_network("DNS", exc)
        except (ConnectionError, TimeoutError, OSError) as exc:
            return self._fail_network("NETWORK", exc)

        self._client = client
        client.loop_start()
        if not got.wait(self.connect_timeout):
            self._teardown()
            return self._fail_network("NO_CONNACK", f"no answer within {self.connect_timeout:.0f} s")
        rc = connack["rc"]
        if rc.is_failure:
            self._teardown()
            if rc.value in AUTH_CODES:
                self.log.error("AUTHENTICATION FAILURE: broker refused the token (%s). "
                               "Not retrying: fix the token and call reset_auth().", rc)
                if self.rec:
                    self.rec.event(self.node_id, "AUTH_FAILURE", str(rc))
                self._set_state("AUTH_FAILED", str(rc))
                return Result.AUTH_FAILURE
            return self._fail_network("CONNECT_REFUSED", rc)

        failed = self._failures
        self.backoff.reset()
        self._failures = 0
        self._set_state("CONNECTED", f"(after {failed} failed attempts)" if failed else "")
        return Result.OK

    # ------------------------------------------------------------------ publishing
    def publish_batch(self, rows, mode="replay"):
        """Publish rows (oldest first). Returns Result; OK only after the QoS 1 PUBACK."""
        if not rows:
            return Result.OK
        res = self.ensure_connected()
        if res is not Result.OK:
            return res
        if mode == "live":
            r = rows[0]
            payload = {"gen_ts_ms": r["ts"]}
            payload.update({k: r[k] for k in VALUE_KEYS})
        else:
            payload = [{"ts": r["ts"], "values": {k: r[k] for k in VALUE_KEYS}} for r in rows]
        t0 = time.time()
        try:
            info = self._client.publish(TOPIC, json.dumps(payload), qos=1)
            info.wait_for_publish(timeout=self.publish_timeout)
            acked = info.is_published()
        except (RuntimeError, ValueError) as exc:
            acked, err = False, exc
        else:
            err = "no PUBACK within timeout"
        t1 = time.time()
        if self.rec:
            self.rec.message(self.node_id, mode, len(rows), rows[0]["ts"], int(t0 * 1000), int(t1 * 1000), acked)
        if acked and not self._lost.is_set():
            return Result.OK
        self._teardown()
        self._lost.set()
        self._failures += 1
        delay = self._schedule_retry()
        self.log.warning("NETWORK FAILURE (publish): %s | batch of %d not confirmed, retry in %.1f s",
                         err, len(rows), delay)
        if self.rec:
            self.rec.event(self.node_id, "NETWORK_FAILURE", f"publish; retry_in={delay:.1f}s")
        self._set_state("DISCONNECTED")
        return Result.NETWORK_FAILURE

    def close(self):
        self._teardown()
