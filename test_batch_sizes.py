
import os
import config
from buffer.stub_buffer import Buffer
from transport import TbPublisher, Backoff

config.load_env()

node_id = "WQ-BID-001"
host = os.environ.get("TB_HOST", "mqtt.thingsboard.cloud")
port = int(os.environ.get("TB_PORT", "8883"))
token = config.token_for(node_id)

if not token:
    raise RuntimeError("Missing device token")

buffer = Buffer("buffer.db")

publisher = TbPublisher(
    node_id,
    token,
    host,
    port,
    tls=True,
    backoff=Backoff(1, 3),
    connect_timeout=10,
    publish_timeout=15
)

try:
    for size in [1, 5, 10, 20, 50]:
        rows = buffer.fetch_unsynced(size, node_id=node_id)

        if len(rows) != size:
            print(f"Skipping {size}: only {len(rows)} rows available")
            continue

        print(f"\nTesting batch size: {size}")

        result = publisher.publish_batch(rows, mode="replay")
        print("Result:", result.value)

        if result.value != "OK":
            print("Stopping after first failure.")
            break

finally:
    publisher.close()
