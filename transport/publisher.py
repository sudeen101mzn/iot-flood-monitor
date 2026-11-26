
"""
MQTT-over-TLS publisher for one ThingsBoard device.

* TLS on port 8883, device access token as MQTT username.
* QoS 1: a publish counts as delivered only after PUBACK.
* NETWORK_FAILURE: retry with exponential backoff.
* AUTH_FAILURE: stop retrying until credentials are corrected.
* Supports simulated outages.
* Supports live and timestamped replay payloads.
* Replay batches are split into chunks of at most 14 records.
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

VALUE_KEYS = (
    "ph",
    "turbidity_ntu",
    "tds_ppm",
    "water_temp_c",
    "flow_lpm",
    "latitude",
    "longitude",
)

AUTH_CODES = (4, 5, 134, 135)
MAX_REPLAY_CHUNK_SIZE = 14


class Result(Enum):
    OK = "OK"
    NETWORK_FAILURE = "NETWORK_FAILURE"
    AUTH_FAILURE = "AUTH_FAILURE"


class TbPublisher:

    def __init__(
        self,
        node_id,
        token,
        host,
        port=8883,
        *,
        tls=True,
        ca_file=None,
        backoff=None,
        connect_timeout=10.0,
        publish_timeout=15.0,
        keepalive=30,
        recorder=None,
    ):

        self.node_id = node_id
        self.token = token
        self.host = host
        self.port = port

        self.tls = tls
        self.ca_file = ca_file

        self.backoff = backoff or Backoff()

        self.connect_timeout = connect_timeout
        self.publish_timeout = publish_timeout
        self.keepalive = keepalive

        self.rec = recorder

        self.log = logging.getLogger(f"transport.{node_id}")

        self.state = "DISCONNECTED"
        self.outage = False
        self.restored_at = None

        self._client = None
        self._lost = threading.Event()

        self._next_attempt = 0.0
        self._failures = 0

    # ---------------------------------------------------------
    # STATE HELPERS
    # ---------------------------------------------------------

    def _set_state(self, new, reason=""):

        if new != self.state:

            self.log.info(
                "STATE %s -> %s %s",
                self.state,
                new,
                reason,
            )

            if self.rec:
                self.rec.event(
                    self.node_id,
                    f"STATE_{new}",
                    reason,
                )

            self.state = new

    def _teardown(self):

        client, self._client = self._client, None

        if client is not None:

            try:
                client.disconnect()
            except Exception:
                pass

            finally:
                try:
                    client.loop_stop()
                except Exception:
                    pass

    def _schedule_retry(self):

        delay = self.backoff.next_delay()
        self._next_attempt = time.monotonic() + delay

        return delay

    # ---------------------------------------------------------
    # OUTAGE TOGGLE
    # ---------------------------------------------------------

    def set_outage(self, on):

        self.outage = on

        if on:

            self.log.warning(
                "OUTAGE ON: network deliberately cut"
            )

            self._teardown()
            self._lost.set()

            self._set_state(
                "DISCONNECTED",
                "(outage_on)",
            )

        else:

            self.log.warning(
                "OUTAGE OFF: network restored "
                "(next scheduled reconnect will succeed)"
            )

            self.restored_at = time.monotonic()

        if self.rec:

            self.rec.event(
                self.node_id,
                "OUTAGE_ON" if on else "OUTAGE_OFF",
            )

    def reset_auth(self, token=None):

        """Call after fixing the token."""

        if token:
            self.token = token

        self.backoff.reset()
        self._next_attempt = 0.0

        self._set_state(
            "DISCONNECTED",
            "(auth reset)",
        )

    # ---------------------------------------------------------
    # CONNECTION
    # ---------------------------------------------------------

    def ensure_connected(self):

        if self.state == "AUTH_FAILED":
            return Result.AUTH_FAILURE

        if (
            self.state == "CONNECTED"
            and self._client is not None
            and not self._lost.is_set()
        ):
            return Result.OK

        if time.monotonic() < self._next_attempt:
            return Result.NETWORK_FAILURE

        return self._attempt_connect()

    def _fail_network(self, kind, exc):

        self._failures += 1

        delay = self._schedule_retry()

        self.log.warning(
            "NETWORK FAILURE (%s): %s | "
            "attempt %d failed, retry in %.1f s",
            kind,
            exc,
            self._failures,
            delay,
        )

        if self.rec:

            self.rec.event(
                self.node_id,
                "NETWORK_FAILURE",
                f"{kind}; retry_in={delay:.1f}s",
            )

        self._set_state("DISCONNECTED")

        return Result.NETWORK_FAILURE

    def _attempt_connect(self):

        self._teardown()
        self._lost.clear()

        self._set_state(
            "CONNECTING",
            f"-> {self.host}:{self.port}",
        )

        connack = {}
        got = threading.Event()

        def on_connect(
            client,
            userdata,
            flags,
            reason_code,
            properties,
        ):

            connack["rc"] = reason_code

            self.log.info(
                "CONNECT DEBUG: broker CONNACK received: %s",
                reason_code,
            )

            if not reason_code.is_failure:
                self._lost.clear()

            got.set()

        def on_disconnect(
            client,
            userdata,
            disconnect_flags,
            reason_code,
            properties,
        ):

            self.log.warning(
                "DISCONNECT DEBUG: reason=%s flags=%s",
                reason_code,
                disconnect_flags,
            )

            if self._client is client:
                self._lost.set()

        try:

            if self.outage:

                raise ConnectionError(
                    "simulated outage (outage_on)"
                )

            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id="",
                protocol=mqtt.MQTTv311,
            )

            client.enable_logger(self.log)

            client.username_pw_set(self.token)

            if self.tls:

                client.tls_set(
                    ca_certs=self.ca_file
                )

            client.on_connect = on_connect
            client.on_disconnect = on_disconnect

            client.connect(
                self.host,
                self.port,
                keepalive=self.keepalive,
            )

        except ssl.SSLError as exc:

            return self._fail_network("TLS", exc)

        except socket.gaierror as exc:

            return self._fail_network("DNS", exc)

        except (
            ConnectionError,
            TimeoutError,
            OSError,
        ) as exc:

            return self._fail_network("NETWORK", exc)

        self._client = client

        client.loop_start()

        if not got.wait(self.connect_timeout):

            self._teardown()

            return self._fail_network(
                "NO_CONNACK",
                f"no answer within {self.connect_timeout:.0f} s",
            )

        rc = connack["rc"]

        if rc.is_failure:

            self._teardown()

            if rc.value in AUTH_CODES:

                self.log.error(
                    "AUTHENTICATION FAILURE: broker refused "
                    "the token (%s). Not retrying.",
                    rc,
                )

                if self.rec:

                    self.rec.event(
                        self.node_id,
                        "AUTH_FAILURE",
                        str(rc),
                    )

                self._set_state(
                    "AUTH_FAILED",
                    str(rc),
                )

                return Result.AUTH_FAILURE

            return self._fail_network(
                "CONNECT_REFUSED",
                rc,
            )

        if self._lost.is_set():

            self._teardown()

            return self._fail_network(
                "CONNECTION_LOST_AFTER_CONNACK",
                "connection lost immediately after connecting",
            )

        failed = self._failures

        self.backoff.reset()
        self._failures = 0

        self._set_state(
            "CONNECTED",
            f"(after {failed} failed attempts)"
            if failed else "",
        )

        return Result.OK

    # ---------------------------------------------------------
    # PUBLISHING
    # ---------------------------------------------------------

    def publish_batch(self, rows, mode="replay"):

        """
        Publish rows oldest first.

        Replay batches are divided into chunks of at most 14.
        Each chunk must receive its own QoS 1 PUBACK.

        Returns OK only when every chunk is acknowledged.
        """

        if not rows:
            return Result.OK

        if mode == "live":

            return self._publish_one_batch(
                rows[:1],
                mode="live",
            )

        total = len(rows)

        if total <= MAX_REPLAY_CHUNK_SIZE:

            return self._publish_one_batch(
                rows,
                mode="replay",
            )

        total_chunks = (
            total + MAX_REPLAY_CHUNK_SIZE - 1
        ) // MAX_REPLAY_CHUNK_SIZE

        self.log.info(
            "REPLAY BATCH: received %d rows; "
            "publishing in chunks of at most %d",
            total,
            MAX_REPLAY_CHUNK_SIZE,
        )

        for index, start in enumerate(
            range(0, total, MAX_REPLAY_CHUNK_SIZE),
            start=1,
        ):

            chunk = rows[
                start:start + MAX_REPLAY_CHUNK_SIZE
            ]

            self.log.info(
                "REPLAY CHUNK %d/%d: publishing %d rows",
                index,
                total_chunks,
                len(chunk),
            )

            result = self._publish_one_batch(
                chunk,
                mode="replay",
            )

            if result is not Result.OK:

                self.log.warning(
                    "REPLAY STOPPED: chunk %d/%d failed. "
                    "Remaining rows will stay in the buffer.",
                    index,
                    total_chunks,
                )

                return result

        self.log.info(
            "REPLAY BATCH COMPLETE: all %d rows acknowledged",
            total,
        )

        return Result.OK

    def _publish_one_batch(self, rows, mode):

        """
        Publish one MQTT message.

        A successful QoS 1 PUBACK is authoritative. A connection
        interruption before that PUBACK does not count as success.
        """

        if not rows:
            return Result.OK

        res = self.ensure_connected()

        if res is not Result.OK:
            return res

        client = self._client

        if client is None or self._lost.is_set():

            return self._fail_network(
                "CONNECTION_LOST",
                "MQTT client unavailable before publishing",
            )

        if mode == "live":

            r = rows[0]

            payload = {
                "gen_ts_ms": r["ts"],
            }

            payload.update({
                k: r[k] for k in VALUE_KEYS
            })

        else:

            payload = [
                {
                    "ts": r["ts"],
                    "values": {
                        k: r[k] for k in VALUE_KEYS
                    },
                }
                for r in rows
            ]

        payload_json = json.dumps(payload)

        payload_bytes = len(
            payload_json.encode("utf-8")
        )

        t0 = time.time()

        acked = False
        err = None
        info = None

        try:

            info = client.publish(
                TOPIC,
                payload_json,
                qos=1,
            )

            self.log.info(
                "PUBLISH DEBUG: mid=%s rc=%s "
                "payload_bytes=%d batch_size=%d mode=%s",
                info.mid,
                info.rc,
                payload_bytes,
                len(rows),
                mode,
            )

            if info.rc != mqtt.MQTT_ERR_SUCCESS:

                err = (
                    f"publish submission failed: "
                    f"MQTT rc={info.rc}"
                )

            else:

                info.wait_for_publish(
                    timeout=self.publish_timeout
                )

                acked = info.is_published()

                self.log.info(
                    "PUBLISH DEBUG: mid=%s published=%s "
                    "lost=%s",
                    info.mid,
                    acked,
                    self._lost.is_set(),
                )

                if not acked:

                    err = (
                        "no PUBACK within "
                        f"{self.publish_timeout:.1f} seconds"
                    )

        except (
            RuntimeError,
            ValueError,
            OSError,
            TimeoutError,
        ) as exc:

            acked = False
            err = repr(exc)

            self.log.warning(
                "PUBLISH DEBUG: mid=%s exception=%r",
                info.mid if info else None,
                exc,
            )

        t1 = time.time()

        if self.rec:

            self.rec.message(
                self.node_id,
                mode,
                len(rows),
                rows[0]["ts"],
                int(t0 * 1000),
                int(t1 * 1000),
                acked,
            )

        # IMPORTANT:
        # If PUBACK was received, delivery is confirmed.
        # A previous disconnect flag must not override that
        # confirmation. Prepare for reconnect before the next
        # publish, but return OK for this acknowledged message.
        if acked:

            if self._lost.is_set():

                self.log.warning(
                    "PUBLISH ACKNOWLEDGED: connection was "
                    "interrupted, but PUBACK confirms delivery. "
                    "Preparing to reconnect for the next publish."
                )

                self._teardown()
                self._lost.clear()

                self._set_state(
                    "DISCONNECTED",
                    "(PUBACK received; reconnect before next publish)",
                )

            return Result.OK

        if err is None:
            err = "PUBACK not confirmed"

        self._teardown()
        self._lost.set()

        self._failures += 1
        delay = self._schedule_retry()

        self.log.warning(
            "NETWORK FAILURE (publish): %s | "
            "batch of %d not confirmed, retry in %.1f s",
            err,
            len(rows),
            delay,
        )

        if self.rec:

            self.rec.event(
                self.node_id,
                "NETWORK_FAILURE",
                f"publish; retry_in={delay:.1f}s",
            )

        self._set_state("DISCONNECTED")

        return Result.NETWORK_FAILURE

    # ---------------------------------------------------------
    # CLOSE
    # ---------------------------------------------------------

    def close(self):

        self._teardown()
