
"""
LIVE MULTI-DEVICE ACQUISITION AND SYNCHRONISATION TEST

Tests:
1. Real WaterQualitySimulator readings from three devices.
2. Database-first acquisition using SQLite.
3. Independent MQTT publishers and SyncWorkers.
4. Automatic live/replay publishing.
5. QoS 1 PUBACK-based synchronisation.
6. Final database verification.

Uses a temporary database. Does not modify buffer.db.
"""

import logging
import tempfile
import time
from pathlib import Path

import config

from sim.simulator import WaterQualitySimulator
from buffer.stub_buffer import Buffer
from transport.publisher import TbPublisher
from transport.backoff import Backoff
from transport.drain import SyncWorker


# ==========================================================
# SETTINGS
# ==========================================================

NODE_IDS = [
    "WQ-BID-001",
    "WQ-BID-002",
    "WQ-BID-018",
]

TB_HOST = "mqtt.thingsboard.cloud"
TB_PORT = 8883

TEST_DURATION_SECONDS = 60
ACQUISITION_INTERVAL_SECONDS = 2

BUFFER_CAPACITY = 1000
DRAIN_TIMEOUT_SECONDS = 90

BATCH_SIZE = 20


# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


# ==========================================================
# MAIN TEST
# ==========================================================

def main():

    print("\n" + "=" * 65)
    print("LIVE MULTI-DEVICE ACQUISITION AND SYNCHRONISATION TEST")
    print("=" * 65)

    passed = False

    # ------------------------------------------------------
    # PHASE 1: LOAD CONFIGURATION
    # ------------------------------------------------------

    config.load_env()

    tokens = {}

    for node_id in NODE_IDS:

        token = config.token_for(node_id)

        if not token:
            raise RuntimeError(
                f"Missing ThingsBoard token for {node_id}. "
                "Check your .env file."
            )

        tokens[node_id] = token

    print("\nSUCCESS: All three device tokens found.")
    print(f"ThingsBoard host: {TB_HOST}:{TB_PORT}")

    # ------------------------------------------------------
    # PHASE 2: INITIALISE SIMULATOR
    # ------------------------------------------------------

    simulator = WaterQualitySimulator()

    print("\nSimulator initialised.")
    print("Devices:", ", ".join(NODE_IDS))

    # ------------------------------------------------------
    # PHASE 3: CREATE TEMPORARY DATABASE
    # ------------------------------------------------------

    with tempfile.TemporaryDirectory() as temp_dir:

        db_path = Path(temp_dir) / "live_multi_device_test.db"

        buffer = Buffer(
            path=str(db_path),
            max_rows=BUFFER_CAPACITY,
        )

        publishers = {}
        workers = {}

        generated = {
            node_id: 0
            for node_id in NODE_IDS
        }

        try:

            print("\nTemporary SQLite database created.")

            # --------------------------------------------------
            # PHASE 4: CREATE ONE PUBLISHER AND WORKER PER DEVICE
            # --------------------------------------------------

            print("\n========== PHASE 1: STARTING WORKERS ==========")

            for node_id in NODE_IDS:

                publisher = TbPublisher(
                    node_id=node_id,
                    token=tokens[node_id],
                    host=TB_HOST,
                    port=TB_PORT,
                    tls=True,
                    backoff=Backoff(
                        min_s=1.0,
                        max_s=30.0,
                    ),
                    connect_timeout=10,
                    publish_timeout=15,
                )

                worker = SyncWorker(
                    buffer=buffer,
                    pub=publisher,
                    node_id=node_id,
                    batch_size=BATCH_SIZE,
                    stamp_mode="auto",
                    live_age_ms=3000,
                )

                publishers[node_id] = publisher
                workers[node_id] = worker

            # Start all workers.
            for node_id in NODE_IDS:

                workers[node_id].start()

                print(
                    f"Started worker: "
                    f"{workers[node_id].name}"
                )

            # --------------------------------------------------
            # PHASE 5: CONTINUOUS SIMULATED ACQUISITION
            # --------------------------------------------------

            print("\n========== PHASE 2: LIVE ACQUISITION ==========")

            print(
                f"Test duration: {TEST_DURATION_SECONDS} seconds"
            )

            print(
                f"Acquisition interval: "
                f"{ACQUISITION_INTERVAL_SECONDS} seconds"
            )

            print("\nGenerating readings...\n")

            start_time = time.monotonic()
            next_sample_time = start_time

            while (
                time.monotonic() - start_time
                < TEST_DURATION_SECONDS
            ):

                for node_id in NODE_IDS:

                    # Generate a reading using the real simulator.
                    reading = simulator.next_reading(node_id)

                    # Database-first acquisition.
                    # The simulator already supplies node_id and ts.
                    buffer.insert_reading(reading)

                    generated[node_id] += 1

                elapsed = time.monotonic() - start_time

                progress = " | ".join(
                    f"{node_id}: {generated[node_id]}"
                    for node_id in NODE_IDS
                )

                print(
                    f"\rElapsed: {elapsed:5.1f}s | {progress}",
                    end="",
                    flush=True,
                )

                next_sample_time += ACQUISITION_INTERVAL_SECONDS

                sleep_time = (
                    next_sample_time - time.monotonic()
                )

                if sleep_time > 0:
                    time.sleep(sleep_time)

            print("\n\nAcquisition completed.")

            # --------------------------------------------------
            # PHASE 6: WAIT FOR SYNCHRONISATION
            # --------------------------------------------------

            print(
                "\n========== PHASE 3: SYNCHRONISATION =========="
            )

            print("Waiting for all workers to drain their buffers...")

            drain_start = time.monotonic()

            while (
                time.monotonic() - drain_start
                < DRAIN_TIMEOUT_SECONDS
            ):

                pending = {
                    node_id: buffer.count_unsynced(node_id)
                    for node_id in NODE_IDS
                }

                total_pending = sum(pending.values())

                print(
                    "\rPending: "
                    + " | ".join(
                        f"{node_id}: {pending[node_id]}"
                        for node_id in NODE_IDS
                    ),
                    end="",
                    flush=True,
                )

                if total_pending == 0:
                    break

                time.sleep(1)

            print("\n")

            # --------------------------------------------------
            # PHASE 7: DATABASE VERIFICATION
            # --------------------------------------------------

            print(
                "\n========== PHASE 4: FINAL VERIFICATION =========="
            )

            total_generated = sum(generated.values())
            total_records = 0
            total_synced = 0
            total_pending = 0

            for node_id in NODE_IDS:

                # Wait for no more database operations before
                # querying the shared SQLite connection.
                with buffer._lock:

                    records = buffer.db.execute(
                        """
                        SELECT COUNT(*)
                        FROM readings
                        WHERE node_id = ?
                        """,
                        (node_id,),
                    ).fetchone()[0]

                    synced = buffer.db.execute(
                        """
                        SELECT COUNT(*)
                        FROM readings
                        WHERE node_id = ? AND synced = 1
                        """,
                        (node_id,),
                    ).fetchone()[0]

                pending = buffer.count_unsynced(node_id)

                total_records += records
                total_synced += synced
                total_pending += pending

                print(f"\nDevice: {node_id}")
                print(f"  Generated:       {generated[node_id]}")
                print(f"  Database rows:   {records}")
                print(f"  Synced:          {synced}")
                print(f"  Unsynced:        {pending}")

            print("\n" + "-" * 65)

            print(f"Total generated:  {total_generated}")
            print(f"Total records:    {total_records}")
            print(f"Total synced:     {total_synced}")
            print(f"Total pending:    {total_pending}")

            # --------------------------------------------------
            # PHASE 8: PASS / FAIL
            # --------------------------------------------------

            passed = (
                total_generated == total_records
                and total_records == total_synced
                and total_pending == 0
            )

            print("\n" + "=" * 65)

            if passed:

                print("SUCCESS: LIVE MULTI-DEVICE TEST PASSED")

                print(
                    "All generated readings were stored "
                    "and synchronised."
                )

                print(
                    "All three devices completed "
                    "their synchronisation."
                )

            else:

                print("TEST INCOMPLETE")

                print(
                    "Some records were missing or remained "
                    "unsynchronised."
                )

            print("=" * 65)

        finally:

            # --------------------------------------------------
            # PHASE 9: CLEAN SHUTDOWN
            # --------------------------------------------------

            print("\n========== CLEAN SHUTDOWN ==========")

            for worker in workers.values():
                worker.stop()

            for worker in workers.values():
                worker.join(timeout=25)

            all_stopped = all(
                not worker.is_alive()
                for worker in workers.values()
            )

            print(f"All workers stopped: {all_stopped}")

            for publisher in publishers.values():
                publisher.close()

            if all_stopped:
                buffer.db.close()
            else:
                print(
                    "WARNING: A worker did not stop within "
                    "25 seconds. Database connection left open "
                    "until temporary-directory cleanup."
                )

            print("Temporary database cleanup completed.")

    return passed


# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":

    try:
        success = main()

    except Exception:

        logging.exception("LIVE MULTI-DEVICE TEST FAILED")
        raise SystemExit(1)

    raise SystemExit(0 if success else 1)
