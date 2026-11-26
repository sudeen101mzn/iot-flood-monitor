
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
# 3. Temporary SQLite database
# --------------------------------------------------

temp_file = tempfile.NamedTemporaryFile(
    suffix=".db",
    delete=False
)

db_path = temp_file.name
temp_file.close()

buffer = Buffer(path=db_path, max_rows=1000)

publisher = None
worker = None


try:

    # --------------------------------------------------
    # 4. Insert synthetic readings
    # --------------------------------------------------

    print("\nInserting 20 test readings...")

    base_ts = int(time.time() * 1000) - 60000

    for i in range(20):

        row = {
            "node_id": node_id,
            "ts": base_ts + i * 1000,
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
    # 5. Create publisher
    # --------------------------------------------------

    publisher = TbPublisher(
        node_id,
        token,
        host,
        port,
        tls=True,
        backoff=Backoff(
            min_s=1,
            max_s=30,
            factor=2,
            jitter=0.2,
        ),
        connect_timeout=10,
        publish_timeout=15,
    )


    # --------------------------------------------------
    # 6. Simulate network outage
    # --------------------------------------------------

    print("\n========== PHASE 1: NETWORK OUTAGE ==========")

    publisher.set_outage(True)

    worker = SyncWorker(
        buffer=buffer,
        pub=publisher,
        node_id=node_id,
        batch_size=20,
        stamp_mode="replay",
    )

    worker.start()

    print("SyncWorker started:", worker.is_alive())
    print("Waiting for failed connection attempts...")

    failure_deadline = time.monotonic() + 20

    while time.monotonic() < failure_deadline:

        if publisher._failures >= 2:
            break

        if not worker.is_alive():
            raise RuntimeError("SyncWorker stopped unexpectedly.")

        time.sleep(0.2)

    if publisher._failures < 2:
        raise RuntimeError("Expected network failures were not observed.")


    # --------------------------------------------------
    # 7. Verify buffer during outage
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

    print("\nBuffer state during outage:")
    print("Total records:", total)
    print("Synced records:", synced)
    print("Unsynced records:", unsynced)
    print("Connection failures observed:", publisher._failures)

    if total != 20 or synced != 0 or unsynced != 20:
        raise RuntimeError("Unexpected buffer state during outage.")

    print("\nSUCCESS: All 20 records remain safely buffered.")


    # --------------------------------------------------
    # 8. Restore simulated network
    # --------------------------------------------------

    print("\n========== PHASE 2: NETWORK RESTORATION ==========")

    publisher.set_outage(False)

    print("Network restored.")
    print("Waiting for automatic reconnection and replay...")

    recovery_deadline = time.monotonic() + 120

    while time.monotonic() < recovery_deadline:

        remaining = buffer.count_unsynced(node_id)

        if remaining == 0:
            # Give SyncWorker time to finish its recovery accounting.
            time.sleep(0.5)
            break

        if not worker.is_alive():
            raise RuntimeError("SyncWorker stopped during recovery.")

        time.sleep(0.2)


    # --------------------------------------------------
    # 9. Stop worker
    # --------------------------------------------------

    worker.stop()
    worker.join(timeout=10)

    if worker.is_alive():
        raise RuntimeError("SyncWorker did not stop cleanly.")


    # --------------------------------------------------
    # 10. Final SQLite verification
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


    print("\n========== FINAL VERIFICATION ==========")

    print("Total records:", total)
    print("Synced records:", synced)
    print("Unsynced records:", unsynced)
    print("Worker stopped:", not worker.is_alive())


    # --------------------------------------------------
    # 11. Final result
    # --------------------------------------------------

    if (
        total == 20
        and synced == 20
        and unsynced == 0
        and not worker.is_alive()
    ):

        print("\nSUCCESS: Network recovery test passed.")
        print("SUCCESS: All 20 buffered readings were synchronised.")
        print("SUCCESS: No test records were lost.")

    else:

        print("\nTEST FAILED OR INCOMPLETE.")


finally:

    if worker is not None and worker.is_alive():
        worker.stop()
        worker.join(timeout=10)

    if publisher is not None:
        publisher.close()

    buffer.db.close()

    # Remove only the temporary test database.
    for suffix in ("", "-wal", "-shm"):
        path = Path(db_path + suffix)

        if path.exists():
            path.unlink()

    print("\nTemporary test database cleaned up.")
