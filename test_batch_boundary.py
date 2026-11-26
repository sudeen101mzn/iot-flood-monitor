
import logging
import os
import sqlite3
import config

from transport import TbPublisher, Backoff
from transport.publisher import Result


# --------------------------------------------------
# 1. Enable detailed logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)


# --------------------------------------------------
# 2. Load ThingsBoard configuration
# --------------------------------------------------

config.load_env()

node_id = "WQ-BID-001"

host = os.environ.get(
    "TB_HOST",
    "mqtt.thingsboard.cloud"
)

port = int(
    os.environ.get("TB_PORT", "8883")
)

token = config.token_for(node_id)

if not token:
    raise RuntimeError("Missing device token")


# --------------------------------------------------
# 3. Read 15 unsynced records from SQLite
#    Database is opened in READ-ONLY mode
# --------------------------------------------------

db_path = "file:buffer.db?mode=ro"

con = sqlite3.connect(db_path, uri=True)

# Return dictionary-like SQLite rows
con.row_factory = sqlite3.Row

keys = [
    "ph",
    "turbidity_ntu",
    "tds_ppm",
    "water_temp_c",
    "flow_lpm",
    "latitude",
    "longitude",
]

columns = ["id", "ts"] + keys

sql = f"""
SELECT {", ".join(columns)}
FROM readings
WHERE synced = 0
  AND node_id = ?
ORDER BY ts ASC
LIMIT 100 OFFSET 50
"""

records = [
    dict(row)
    for row in con.execute(sql, (node_id,)).fetchall()
]

con.close()

if len(records) < 20:
    raise RuntimeError(
        f"Not enough unsynced records. Found: {len(records)}"
    )

# Keep exactly the same 15 records for both tests
remaining_records = records[:20]

print(f"\nLoaded {len(remaining_records)} records from SQLite.")


# --------------------------------------------------
# 4. Create MQTT publisher
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
# 5. Publish in two separate batches: 14 + 1
# --------------------------------------------------

try:

    for size in [20]:

        batch = remaining_records[:size]

        remaining_records = remaining_records[size:]

        print(
            f"\nTesting {len(batch)} replay records..."
        )

        result = publisher.publish_batch(
            batch,
            mode="replay"
        )

        print(f"Result: {result}")

        if result != Result.OK:
            print("Stopping after failure.")
            break

    else:
        print(
            "\nBoth batches published successfully."
        )

finally:

    # Always close the MQTT publisher
    publisher.close()

    print("\nPublisher closed.")
