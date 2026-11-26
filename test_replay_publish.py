
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
    raise RuntimeError("Device token not found in .env")

buffer = Buffer("buffer.db")
rows = buffer.fetch_unsynced(1, node_id=node_id)

if not rows:
    raise RuntimeError("No unsynced readings available")

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
    print("Testing one replay record...")
    print("Node:", node_id)
    print("Rows:", len(rows))

    result = publisher.publish_batch(rows, mode="replay")
    print("Publish result:", result.value)

finally:
    publisher.close()
