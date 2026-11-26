
"""Multi-device integration test:
- Three independent ThingsBoard publishers and SyncWorkers.
- One shared SQLite buffer, filtered by node_id.
- Simulated outage on WQ-BID-002 only.
- Verify the other two devices continue synchronising.
- Restore WQ-BID-002 and verify automatic recovery.
"""

import logging
import os
import tempfile
import time
from pathlib import Path

import config
from buffer.stub_buffer import Buffer
from transport.backoff import Backoff
from transport.publisher import TbPublisher
from transport.drain import SyncWorker


NODES = [
    "WQ-BID-001",
    "WQ-BID-002",
    "WQ-BID-018",
]

OUTAGE_NODE = "WQ-BID-002"

INITIAL_READINGS = 5
ADDITIONAL_READINGS = 3

BATCH_SIZE = 20


def make_reading(node_id, index):
    """Create one synthetic telemetry reading for a test device."""

    coordinates = {
        "WQ-BID-001": (27.9115, 85.1462),
        "WQ-BID-002": (27.9158, 85.1510),
        "WQ-BID-018": (27.9072, 85.1415),
    }

    latitude, longitude = coordinates[node_id]

    return {
        "node_id": node_id,
        "ts": int(time.time() * 1000) - 120000 + index,
        "ph": 7.2 + (index % 3) * 0.01,
        "turbidity_ntu": 4.0 + index * 0.1,
        "tds_ppm": 180 + index,
        "water_temp_c": 22.0 + index * 0.1,
        "flow_lpm": 13.0 + index * 0.1,
        "latitude": latitude,
        "longitude": longitude,
    }


def wait_until(condition, timeout, description):
    """Wait until a condition becomes true or the timeout expires."""

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if condition():
            print(f"SUCCESS: {description}")
            return True

        time.sleep(0.2)

    print(f"TIMEOUT: {description}")
    return False


def show_buffer_state(buffer, title):
    print(f"\n{title}")

    for node in NODES:
        pending = buffer.count_unsynced(node)
        print(f"{node}: {pending} unsynced records")


def main():
    config.load_env()

    host = os.environ.get("TB_HOST", "mqtt.thingsboard.cloud")
    port = int(os.environ.get("TB_PORT", "8883"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    print("=" * 65)
    print("MULTI-DEVICE INTEGRATION TEST")
    print("=" * 65)

    # Validate all tokens before starting.
    tokens = {}

    for node in NODES:
        token = config.token_for(node)

        if not token:
            raise RuntimeError(
                f"Missing token for {node}. "
                "Check the corresponding entry in your local .env file."
            )

        tokens[node] = token

    print("All three device tokens found.")
    print(f"ThingsBoard host: {host}:{port}")

    publishers = {}
    workers = []
    buffer = None

    try:
        # Use a temporary directory and database.
        temp_dir = tempfile.TemporaryDirectory(
            prefix="thingsboard_multi_test_"
        )
        db_path = Path(temp_dir.name) / "multi_device_test.db"

        buffer = Buffer(str(db_path), max_rows=1000)

        print("\nTemporary SQLite database created.")

        # Create one publisher and worker per device.
        for node in NODES:
            publisher = TbPublisher(
                node,
                tokens[node],
                host,
                port,
                tls=True,
                backoff=Backoff(
                    min_s=1.0,
                    max_s=8.0,
                    factor=2.0,
                    jitter=0.2,
                ),
                connect_timeout=10,
                publish_timeout=15,
            )

            publishers[node] = publisher

            worker = SyncWorker(
                buffer,
                publisher,
                node,
                batch_size=BATCH_SIZE,
                stamp_mode="replay",
            )

            workers.append(worker)

        # --------------------------------------------------
        # PHASE 1: Insert initial readings for all devices.
        # --------------------------------------------------

        print("\n========== PHASE 1: INITIAL READINGS ==========")

        for node in NODES:
            for i in range(INITIAL_READINGS):
                buffer.insert_reading(make_reading(node, i))

        show_buffer_state(buffer, "Initial buffer state:")

        # --------------------------------------------------
        # PHASE 2: Simulate outage on device 002 only.
        # --------------------------------------------------

        print("\n========== PHASE 2: DEVICE-SPECIFIC OUTAGE ==========")

        publishers[OUTAGE_NODE].set_outage(True)

        print(f"Simulated outage enabled for {OUTAGE_NODE}.")

        # Start all three workers.
        for worker in workers:
            worker.start()
            print(f"Started worker: {worker.name}")

        # Wait for at least two failed connection attempts.
        outage_detected = wait_until(
            lambda: publishers[OUTAGE_NODE]._failures >= 2,
            timeout=20,
            description="Two connection failures observed for WQ-BID-002",
        )

        if not outage_detected:
            raise RuntimeError("The simulated outage was not detected.")

        # The unaffected devices should drain their initial readings.
        healthy_drained = wait_until(
            lambda: (
                buffer.count_unsynced("WQ-BID-001") == 0
                and buffer.count_unsynced("WQ-BID-018") == 0
            ),
            timeout=60,
            description="WQ-BID-001 and WQ-BID-018 synchronised",
        )

        if not healthy_drained:
            raise RuntimeError(
                "One or both unaffected devices failed to synchronise."
            )

        # The affected device must retain its readings.
        if buffer.count_unsynced(OUTAGE_NODE) != INITIAL_READINGS:
            raise RuntimeError(
                "Unexpected buffer state for the device under outage."
            )

        print(
            f"SUCCESS: {OUTAGE_NODE} retained all "
            f"{INITIAL_READINGS} readings."
        )

        # --------------------------------------------------
        # PHASE 3: Continue generating readings for healthy
        # devices while the third device remains offline.
        # --------------------------------------------------

        print("\n========== PHASE 3: INDEPENDENT OPERATION ==========")

        for node in ("WQ-BID-001", "WQ-BID-018"):
            for i in range(ADDITIONAL_READINGS):
                buffer.insert_reading(
                    make_reading(node, 100 + i)
                )

        print("Inserted additional readings for the two healthy devices.")

        healthy_second_wave = wait_until(
            lambda: (
                buffer.count_unsynced("WQ-BID-001") == 0
                and buffer.count_unsynced("WQ-BID-018") == 0
            ),
            timeout=60,
            description="Both healthy devices continued synchronising",
        )

        if not healthy_second_wave:
            raise RuntimeError(
                "The healthy devices did not synchronise the additional readings."
            )

        if buffer.count_unsynced(OUTAGE_NODE) != INITIAL_READINGS:
            raise RuntimeError(
                "The affected device's buffer changed unexpectedly."
            )

        show_buffer_state(
            buffer,
            "Buffer state while WQ-BID-002 remains offline:",
        )

        # --------------------------------------------------
        # PHASE 4: Restore the affected device.
        # --------------------------------------------------

        print("\n========== PHASE 4: NETWORK RESTORATION ==========")

        publishers[OUTAGE_NODE].set_outage(False)

        print(f"Outage disabled for {OUTAGE_NODE}.")
        print("Waiting for automatic reconnection and replay...")

        recovered = wait_until(
            lambda: buffer.count_unsynced(OUTAGE_NODE) == 0,
            timeout=120,
            description="WQ-BID-002 backlog synchronised",
        )

        if not recovered:
            raise RuntimeError(
                "WQ-BID-002 did not synchronise after restoration."
            )

        # --------------------------------------------------
        # PHASE 5: Final database verification.
        # --------------------------------------------------

        print("\n========== PHASE 5: FINAL VERIFICATION ==========")

        cursor = buffer.db.execute(
            """
            SELECT node_id, COUNT(*), SUM(synced)
            FROM readings
            GROUP BY node_id
            ORDER BY node_id
            """
        )

        results = cursor.fetchall()

        expected = {
            "WQ-BID-001": INITIAL_READINGS + ADDITIONAL_READINGS,
            "WQ-BID-002": INITIAL_READINGS,
            "WQ-BID-018": INITIAL_READINGS + ADDITIONAL_READINGS,
        }

        all_passed = True

        for node, total, synced in results:
            synced = synced or 0
            unsynced = total - synced

            print(
                f"{node}: total={total}, "
                f"synced={synced}, unsynced={unsynced}"
            )

            if (
                total != expected[node]
                or synced != expected[node]
                or unsynced != 0
            ):
                all_passed = False

        print("\nStopping all workers...")

        for worker in workers:
            worker.stop()

        for worker in workers:
            worker.join(timeout=5)

        workers_stopped = all(not worker.is_alive() for worker in workers)

        print(f"All workers stopped: {workers_stopped}")

        if all_passed and workers_stopped:
            print("\n" + "=" * 65)
            print("SUCCESS: MULTI-DEVICE INTEGRATION TEST PASSED")
            print("All three devices operated independently.")
            print("One device's outage did not stop the other devices.")
            print("The affected device recovered automatically.")
            print("All expected records were synchronised.")
            print("=" * 65)
        else:
            raise RuntimeError("Final verification failed.")

    finally:
        # Stop any workers still running.
        for worker in workers:
            if worker.ident is not None and worker.is_alive():
                worker.stop()

        for worker in workers:
            if worker.ident is not None:
                worker.join(timeout=5)

        # Close database and remove only the temporary test files.
        if buffer is not None:
            buffer.db.close()

        if "temp_dir" in locals():
            temp_dir.cleanup()

        print("\nTemporary test database cleaned up.")


if __name__ == "__main__":
    main()
