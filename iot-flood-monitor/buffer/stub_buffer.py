"""PLACEHOLDER buffer so part D can be tested alone. Sabin's real store replaces this file;
it only has to offer the same four methods (node_id filter included).

SQLite in WAL mode, rows are written BEFORE any publish attempt and marked synced afterwards.
Capacity: when max_rows is reached the OLDEST rows are evicted (synced ones first). Sabin owns the
real policy and its justification.
"""
import logging
import sqlite3
import threading

COLUMNS = ("node_id", "ts", "ph", "turbidity_ntu", "tds_ppm", "water_temp_c", "flow_lpm", "latitude", "longitude")


class Buffer:
    def __init__(self, path="buffer.db", max_rows=10000):
        self.max_rows = max_rows
        self._lock = threading.Lock()
        self.log = logging.getLogger("buffer")
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS readings(
            id INTEGER PRIMARY KEY AUTOINCREMENT, node_id TEXT NOT NULL, ts INTEGER NOT NULL,
            ph REAL, turbidity_ntu REAL, tds_ppm INTEGER, water_temp_c REAL, flow_lpm REAL,
            latitude REAL, longitude REAL, synced INTEGER NOT NULL DEFAULT 0)""")
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_unsynced ON readings(synced, node_id, ts)")
        self.db.commit()

    def insert_reading(self, row):
        with self._lock:
            cur = self.db.execute(
                f"INSERT INTO readings({','.join(COLUMNS)}) VALUES({','.join('?' * len(COLUMNS))})",
                [row[c] for c in COLUMNS])
            self.db.commit()
            total = self.db.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
            if total > self.max_rows:
                self._evict(total - self.max_rows)
            return cur.lastrowid

    def _evict(self, n):
        self.db.execute("""DELETE FROM readings WHERE id IN (
            SELECT id FROM readings ORDER BY synced DESC, ts ASC LIMIT ?)""", (n,))
        self.db.commit()
        self.log.warning("EVICT: buffer full, dropped %d oldest rows", n)

    def fetch_unsynced(self, limit, node_id=None):
        q = "SELECT id," + ",".join(COLUMNS) + " FROM readings WHERE synced=0"
        args = []
        if node_id:
            q += " AND node_id=?"
            args.append(node_id)
        q += " ORDER BY ts ASC LIMIT ?"
        args.append(limit)
        with self._lock:
            cur = self.db.execute(q, args)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]

    def mark_synced(self, ids):
        with self._lock:
            self.db.executemany("UPDATE readings SET synced=1 WHERE id=?", [(i,) for i in ids])
            self.db.commit()

    def count_unsynced(self, node_id=None):
        q, args = "SELECT COUNT(*) FROM readings WHERE synced=0", []
        if node_id:
            q += " AND node_id=?"
            args.append(node_id)
        with self._lock:
            return self.db.execute(q, args).fetchone()[0]
