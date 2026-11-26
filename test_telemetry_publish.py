
import os
import config

from sim.simulator import WaterQualitySimulator
from transport.publisher import TbPublisher, Result

config.load_env()

host = os.environ.get("TB_HOST")
port = int(os.environ.get("TB_PORT", "8883"))

sim = WaterQualitySimulator(
    scenario_path="scenarios/demo_scenario.json"
)

print("ThingsBoard Telemetry Publishing Test")
print("--------------------------------------")

for node_id in sim.nodes:
    token = config.token_for(node_id)

    if not token:
        print(f"{node_id}: TOKEN MISSING")
        continue

    publisher = TbPublisher(
        node_id=node_id,
        token=token,
        host=host,
        port=port,
        tls=True
    )

    try:
        reading = sim.next_reading(node_id)

        result = publisher.publish_batch(
            [reading],
            mode="live"
        )

        print(f"{node_id}: {result.value}")

    except Exception as exc:
        print(f"{node_id}: ERROR ({type(exc).__name__})")

    finally:
        publisher.close()

print("--------------------------------------")
print("Test completed.")
