
import logging
import os
import tempfile
import time
from pathlib import Path

import config

from buffer.stub_buffer import Buffer
from transport import TbPublisher, Backoff
from transport.publisher import Result
from transport.drain import drain


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
# 3. Create a separate temporary SQLite database
# --------------------------------------------------

temp_file = tempfile.NamedTemporaryFile(
    suffix=".db",
    delete=False
)

db_path = temp_file.name
temp_file.close()

buffer = Buffer(path=db_path, max_rows=1000)

publisher = None


try:

    # --------------------------------------------------
    # 4. Insert 20 synthetic test readings
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

    print("Inserted:", buffer.count_unsynced(node_id))


    # --------------------------------------------------
    # 5. Create the real MQTT publisher
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


    # --------------------------------------------------
    # 6. Run the actual drain function
    # --------------------------------------------------

    print("\nStarting SQLite -> MQTT drain test...")

    stats = drain(
        buffer=buffer,
        pub=publisher,
        node_id=node_id,
        batch_size=20,
        stamp_mode="replay",
    )

    print("\nDrain result:", stats.result)
    print("Rows marked synced:", stats.rows)
    print("Successful drain batches:", stats.batches)


    # --------------------------------------------------
    # 7. Verify SQLite state
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


    print("\n========== SQLITE VERIFICATION ==========")

    print("Total records:", total)
    print("Synced records:", synced)
    print("Unsynced records:", unsynced)


    # --------------------------------------------------
    # 8. Final assessment
    # --------------------------------------------------

    if (
        stats.result is Result.OK
        and stats.rows == 20
        and total == 20
        and synced == 20
        and unsynced == 0
    ):

        print("\nSUCCESS: All 20 records were acknowledged and marked synced.")
        print("SUCCESS: All records remain stored in SQLite.")

    else:

        print("\nTEST INCOMPLETE OR FAILED.")
        print("Inspect the MQTT logs and SQLite verification above.")


finally:

    if publisher is not None:
        publisher.close()

    buffer.db.close()

    # Remove only this test's temporary database.
    for suffix in ("", "-wal", "-shm"):
        path = Path(db_path + suffix)
        if path.exists():
            path.unlink()

    print("\nTemporary test database cleaned up.")
