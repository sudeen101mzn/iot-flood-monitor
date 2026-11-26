
import logging
import os
import tempfile
import time
from pathlib import Path

import config

from buffer.stub_buffer import Buffer
from transport import TbPublisher, Backoff
from transport.drain import SyncWorker


# --------------------------------------------------
# 1. Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)


# --------------------------------------------------
# 2. ThingsBoard configuration
# --------------------------------------------------

config.load_env()

node_id = "WQ-BID-001"

host = os.environ.get("TB_HOST", "mqtt.thingsboard.cloud")
port = int(os.environ.get("TB_PORT", "8883"))

token = config.token_for(node_id)

if not token:
    raise RuntimeError("Missing device token")


# --------------------------------------------------
# 3. Create isolated temporary SQLite database
# --------------------------------------------------

temp_file = tempfile.NamedTemporaryFile(
    suffix=".db",
    delete=False
)

db_path = temp_file.name
temp_file.close()

buffer = Buffer(path=db_path, max_rows=1000)
worker = None


try:

    # --------------------------------------------------
    # 4. Insert 20 synthetic readings
    # --------------------------------------------------

    print("\nInserting 20 test readings...")

    base_ts = int(time.time() * 1000) - 60000

    for i in range(20):

        row = {
            "node_id": node_id,
            "ts": base_ts + (i * 1000),
            "ph": 7.2,
            "turbidity_ntu": 2.1,
            "tds_ppm": 180,
            "water_temp_c": 22.5,
            "flow_lpm": 1.2,
            "latitude": 27.7172,
            "longitude": 85.3240,
        }

        buffer.insert_reading(row)

    print("Initial unsynced records:", buffer.count_unsynced(node_id))


    # --------------------------------------------------
    # 5. Create publisher and background worker
    # --------------------------------------------------

    publisher = TbPublisher(
        node_id,
        token,
        host,
        port,
        tls=True,
        backoff=Backoff(1, 3),
        connect_timeout=10,
        publish_timeout=15,
    )

    worker = SyncWorker(
        buffer=buffer,
        pub=publisher,
        node_id=node_id,
        batch_size=20,
        stamp_mode="replay",
        live_age_ms=3000,
    )


    # --------------------------------------------------
    # 6. Start SyncWorker
    # --------------------------------------------------

    print("\nStarting background SyncWorker...")

    worker.start()

    print("Worker started:", worker.is_alive())


    # --------------------------------------------------
    # 7. Wait for automatic synchronisation
    # --------------------------------------------------

    timeout_seconds = 90
    start_time = time.monotonic()

    while time.monotonic() - start_time < timeout_seconds:

        remaining = buffer.count_unsynced(node_id)

        if remaining == 0:
            break

        if not worker.is_alive():
            print("WARNING: SyncWorker stopped unexpectedly.")
            break

        time.sleep(0.2)


    # --------------------------------------------------
    # 8. Stop worker cleanly
    # --------------------------------------------------

    worker.stop()
    worker.join(timeout=10)

    if worker.is_alive():
        raise RuntimeError("SyncWorker did not stop within 10 seconds.")


    # --------------------------------------------------
    # 9. Verify SQLite records
    # --------------------------------------------------

    total = buffer.db.execute(
        "SELECT COUNT(*) FROM readings WHERE node_id=?",
        (node_id,)
    ).fetchone()[0]

    synced = buffer.db.execute(
        "SELECT COUNT(*) FROM readings WHERE node_id=? AND synced=1",
        (node_id,)
    ).fetchone()[0]

    unsynced = buffer.count_unsynced(node_id)


    print("\n========== SYNCWORKER VERIFICATION ==========")

    print("Total records:", total)
    print("Synced records:", synced)
    print("Unsynced records:", unsynced)
    print("Worker stopped:", not worker.is_alive())


    # --------------------------------------------------
    # 10. Final assessment
    # --------------------------------------------------

    if (
        total == 20
        and synced == 20
        and unsynced == 0
        and not worker.is_alive()
    ):

        print("\nSUCCESS: SyncWorker automatically drained all 20 records.")
        print("SUCCESS: All records were retained in SQLite.")
        print("SUCCESS: Background worker stopped cleanly.")

    else:

        print("\nTEST FAILED OR INCOMPLETE.")
        print("Review the logs and verification results.")


finally:

    if worker is not None and worker.is_alive():
        worker.stop()
        worker.join(timeout=10)

    buffer.db.close()

    # Remove only this test's temporary database.
    for suffix in ("", "-wal", "-shm"):
        path = Path(db_path + suffix)
        if path.exists():
            path.unlink()

    print("\nTemporary test database cleaned up.")
