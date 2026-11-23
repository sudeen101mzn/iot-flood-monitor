"""CSV metrics recorder (thread-safe). Writes into logs/:

messages.csv : one row per publish  (node, mode, batch_size, first_gen_ts_ms, publish_ms, puback_ms, ok)
events.csv   : state transitions     (wall_ms, node, event, detail)
flush.csv    : one row per completed buffer drain after an outage
               (node, rows, seconds, rows_per_s, backlog_at_start, recovery_s)
generated.csv: one row per generated reading (node, ts_ms) for delivery-ratio / data-loss checks
depth.csv    : buffer depth (unsynced rows) once per second per node, for the buffer-depth plot
"""
import csv
import threading
import time
from pathlib import Path

FILES = {
    "messages": ["node", "mode", "batch_size", "first_gen_ts_ms", "publish_ms", "puback_ms", "ok"],
    "events": ["wall_ms", "node", "event", "detail"],
    "flush": ["node", "rows", "seconds", "rows_per_s", "backlog_at_start", "recovery_s"],
    "generated": ["node", "ts_ms"],
    "depth": ["wall_ms", "node", "unsynced"],
}


class MetricsRecorder:
    def __init__(self, directory="logs", fresh=True):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        for name, header in FILES.items():
            path = self.dir / f"{name}.csv"
            if fresh or not path.exists():
                with open(path, "w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(header)

    def _write(self, name, row):
        with self._lock, open(self.dir / f"{name}.csv", "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)

    def depth(self, node, unsynced):
        self._write("depth", [int(time.time() * 1000), node, unsynced])

    def generated(self, node, ts_ms):
        self._write("generated", [node, ts_ms])

    def message(self, node, mode, batch_size, first_gen_ts_ms, publish_ms, puback_ms, ok):
        self._write("messages", [node, mode, batch_size, first_gen_ts_ms, publish_ms, puback_ms, int(ok)])

    def event(self, node, event, detail=""):
        self._write("events", [int(time.time() * 1000), node, event, detail])

    def flush(self, node, rows, seconds, backlog_at_start, recovery_s):
        rate = rows / seconds if seconds > 0 else 0.0
        self._write("flush", [node, rows, round(seconds, 3), round(rate, 1), backlog_at_start,
                              "" if recovery_s is None else round(recovery_s, 3)])
