"""Drain loop: SQLite buffer -> MQTT, in batches, oldest first (written jointly with part B).

Rules (from the brief): the database is the source of truth; a row is marked synced only
after the QoS 1 PUBACK; keep draining in batches until the buffer is empty or a publish fails.
Live readings (buffer holds only one fresh row) go server-stamped so end-to-end latency can
be measured; everything else is replayed client-stamped with its ORIGINAL timestamp.
"""
import logging
import threading
import time
from dataclasses import dataclass

from .publisher import Result


@dataclass
class DrainStats:
    rows: int = 0
    batches: int = 0
    result: Result = Result.OK


def drain(buffer, pub, node_id, batch_size=50, stamp_mode="auto", live_age_ms=3000):
    stats = DrainStats()
    while True:
        rows = buffer.fetch_unsynced(batch_size, node_id=node_id)     # oldest first
        if not rows:
            return stats
        age_ms = int(time.time() * 1000) - rows[0]["ts"]
        live = stamp_mode == "auto" and len(rows) == 1 and age_ms <= live_age_ms
        res = pub.publish_batch(rows, "live" if live else "replay")
        stats.result = res
        if res is not Result.OK:
            return stats
        buffer.mark_synced([r["id"] for r in rows])                   # only after PUBACK
        stats.rows += len(rows)
        stats.batches += 1


class SyncWorker(threading.Thread):
    """One thread per node, so a dead network can never block data acquisition."""

    def __init__(self, buffer, pub, node_id, batch_size=50, stamp_mode="auto",
                 live_age_ms=3000, recorder=None):
        super().__init__(name=f"sync-{node_id}", daemon=True)
        self.buffer, self.pub, self.node_id = buffer, pub, node_id
        self.batch_size, self.stamp_mode, self.live_age_ms = batch_size, stamp_mode, live_age_ms
        self.rec = recorder
        self.stop_event = threading.Event()
        self.log = logging.getLogger(f"sync.{node_id}")

    def run(self):
        failing = False          # True while we have seen a failure and not yet recovered
        session = None           # one flush session = first success after failure -> buffer empty
        warned_auth = False
        while not self.stop_event.is_set():
            backlog = self.buffer.count_unsynced(self.node_id)
            if backlog == 0:
                self.stop_event.wait(0.1)
                continue
            if failing and session is None:
                pass
            t_before = time.monotonic()
            stats = drain(self.buffer, self.pub, self.node_id, self.batch_size, self.stamp_mode, self.live_age_ms)
            if stats.result is Result.AUTH_FAILURE:
                if not warned_auth:
                    self.log.error("sync paused: authentication failure (buffer keeps filling, nothing is lost)")
                    warned_auth = True
                failing = True
                self.stop_event.wait(1.0)
                continue
            if stats.result is Result.NETWORK_FAILURE:
                if not failing:
                    self.log.warning("sync: delivery failing, buffering locally (backlog=%d)", backlog)
                failing = True
                if stats.rows and session:        # partial drain interrupted again
                    session["rows"] += stats.rows
                self.stop_event.wait(0.3)
                continue
            # result OK: everything fetched so far was delivered
            if failing and stats.rows:
                if session is None:
                    session = {"t0": t_before, "rows": 0, "backlog": backlog}
                    self.log.info("sync: connection back, replaying backlog of %d rows (batch=%d)", backlog, self.batch_size)
                session["rows"] += stats.rows
            if session and self.buffer.count_unsynced(self.node_id) == 0:
                secs = time.monotonic() - session["t0"]
                rec_s = (time.monotonic() - self.pub.restored_at) if self.pub.restored_at else None
                rate = session["rows"] / secs if secs > 0 else 0
                self.log.info("DRAIN COMPLETE: %d rows in %.2f s (%.0f rows/s)%s", session["rows"], secs, rate,
                              f", recovery {rec_s:.1f} s after outage_off" if rec_s is not None else "")
                if self.rec:
                    self.rec.flush(self.node_id, session["rows"], secs, session["backlog"], rec_s)
                session, failing, warned_auth = None, False, False
                self.pub.restored_at = None
        self.pub.close()

    def stop(self):
        self.stop_event.set()
