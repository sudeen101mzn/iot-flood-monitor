
"""Demo runner (part D): acquisition loop + one sync worker per node + scenario commands.

Commands:
  start
  trigger_intrusion [node]
  outage_on
  outage_off
  status
  supply_interruption_on [node]
  supply_interruption_off [node]
  reset
  quit

Acquisition never waits for the network: every reading goes into the SQLite
buffer first, and the per-node SyncWorker threads publish from there.
"""

import argparse
import logging
import queue
import sys
import threading
import time
from pathlib import Path

import config
from metrics.recorder import MetricsRecorder
from transport import Backoff, SyncWorker, TbPublisher

from sim import WaterQualitySimulator

try:
    # Sabin's real buffer when present
    from buffer import Buffer
except ImportError:
    # Temporary placeholder buffer
    from buffer.stub_buffer import Buffer


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    p.add_argument(
        "--nodes",
        help="comma-separated node IDs (default: all nodes in scenario file)"
    )
    p.add_argument(
        "--scenario-file",
        default="scenarios/demo_scenario.json",
        help="simulator configuration"
    )
    p.add_argument("--host")
    p.add_argument("--port", type=int)
    p.add_argument(
        "--no-tls",
        action="store_true",
        help="plain MQTT (local testing only)"
    )
    p.add_argument(
        "--ca-file",
        help="CA certificate for a self-signed test broker"
    )
    p.add_argument(
        "--tick",
        type=float,
        default=1.0,
        help="seconds between readings"
    )
    p.add_argument(
        "--time-scale",
        type=float,
        help="override scenario time_scale"
    )
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument(
        "--stamp-mode",
        choices=["auto", "client"],
        default="auto",
        help="auto: live=server-stamped, replay=client-stamped"
    )
    p.add_argument("--backoff-min", type=float, default=1.0)
    p.add_argument("--backoff-max", type=float, default=30.0)
    p.add_argument("--db", default="buffer.db")
    p.add_argument("--max-rows", type=int, default=10000)
    p.add_argument(
        "--script",
        help="scenario script with timed commands"
    )
    p.add_argument("--autostart", action="store_true")
    p.add_argument(
        "--duration",
        type=float,
        help="stop after N seconds"
    )
    p.add_argument("--no-stdin", action="store_true")
    p.add_argument("--log-dir", default="logs")

    return p.parse_args()


def load_script(path):
    """Load timed commands from a scenario script."""

    steps = []

    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.split("#")[0].strip()

        if not line:
            continue

        parts = line.split(None, 1)

        if len(parts) != 2:
            raise ValueError(
                f"Invalid scenario script line: {line}"
            )

        timestamp = float(parts[0])
        command = parts[1].strip()

        steps.append((timestamp, command))

    return sorted(steps)


def stdin_reader(command_queue):
    """Read terminal commands and place them into the queue."""

    try:
        for line in sys.stdin:
            command = line.strip()

            if command:
                command_queue.put(command)

    except (EOFError, OSError):
        pass


def main():
    args = parse_args()

    config.load_env()

    import os

    host = args.host or os.environ.get(
        "TB_HOST",
        "demo.thingsboard.io"
    )

    port = args.port or int(
        os.environ.get("TB_PORT", "8883")
    )

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s.%(msecs)03d "
            "%(levelname)-7s %(name)s: %(message)s"
        ),
        datefmt="%H:%M:%S"
    )

    log = logging.getLogger("runner")

    # Metrics recorder
    rec = MetricsRecorder(args.log_dir)

    # Simulator
    sim = WaterQualitySimulator(
        scenario_path=args.scenario_file
    )

    if args.time_scale:
        sim.set_time_scale(args.time_scale)

    # Selected nodes
    nodes = (
        [n.strip() for n in args.nodes.split(",") if n.strip()]
        if args.nodes
        else list(sim.nodes)
    )

    # Validate selected nodes
    unknown_nodes = [n for n in nodes if n not in sim.nodes]

    if unknown_nodes:
        raise ValueError(
            f"Unknown node IDs: {unknown_nodes}. "
            f"Available nodes: {list(sim.nodes)}"
        )

    # SQLite buffer
    buf = Buffer(
        args.db,
        max_rows=args.max_rows
    )

    pubs = {}
    workers = []

    # One publisher and worker per node
    for n in nodes:

        token = config.token_for(n)

        if not token:
            log.warning(
                "No token for %s. "
                "Set TB_TOKEN_%s in .env; using a dummy token.",
                n,
                n.replace("-", "_")
            )

        pub = TbPublisher(
            n,
            token or "dummy",
            host,
            port,
            tls=not args.no_tls,
            ca_file=args.ca_file,
            backoff=Backoff(
                args.backoff_min,
                args.backoff_max
            ),
            recorder=rec
        )

        pubs[n] = pub

        worker = SyncWorker(
            buf,
            pub,
            n,
            args.batch_size,
            args.stamp_mode,
            recorder=rec
        )

        worker.start()
        workers.append(worker)

    # Command queue
    cmds = queue.Queue()

    # Start terminal input thread
    if not args.no_stdin:
        input_thread = threading.Thread(
            target=stdin_reader,
            args=(cmds,),
            daemon=True,
            name="stdin-reader"
        )
        input_thread.start()

    # Timed scenario script
    script = load_script(args.script) if args.script else []

    running = args.autostart
    t_start = time.monotonic()

    log.info(
        "Runner ready: nodes=%s host=%s:%d tick=%.1fs time_scale=%.0f. "
        "Type 'start' to begin.",
        nodes,
        host,
        port,
        args.tick,
        sim.time_scale
    )

    # ---------------------------------------------------------
    # COMMAND HANDLER
    # ---------------------------------------------------------

    def handle(line):
        nonlocal running

        line = line.strip()

        if not line:
            return True

        parts = line.split()

        if not parts:
            return True

        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else None

        # Reject unexpected extra arguments
        if len(parts) > 2:
            log.warning(
                "Too many arguments for command: %s",
                line
            )
            return True

        log.info("COMMAND %s", line)
        rec.event("runner", "COMMAND", line)

        try:

            if cmd == "start":

                running = True
                log.info("Acquisition started.")

            elif cmd == "trigger_intrusion":

                target = arg or getattr(
                    sim,
                    "default_intrusion_node",
                    None
                )

                if target and target not in nodes:
                    log.warning(
                        "Node %s is not selected in this run.",
                        target
                    )
                    return True

                sim.trigger_intrusion(target)

                log.info(
                    "Intrusion scenario triggered for %s.",
                    target or "default node"
                )

            elif cmd == "supply_interruption_on":

                target = arg or getattr(
                    sim,
                    "default_interruption_node",
                    None
                )

                if target and target not in nodes:
                    log.warning(
                        "Node %s is not selected in this run.",
                        target
                    )
                    return True

                sim.set_supply_interruption(
                    True,
                    target
                )

                log.info(
                    "Supply interruption activated for %s.",
                    target or "default node"
                )

            elif cmd == "supply_interruption_off":

                target = arg or getattr(
                    sim,
                    "default_interruption_node",
                    None
                )

                if target and target not in nodes:
                    log.warning(
                        "Node %s is not selected in this run.",
                        target
                    )
                    return True

                sim.set_supply_interruption(
                    False,
                    target
                )

                log.info(
                    "Supply interruption deactivated for %s.",
                    target or "default node"
                )

            elif cmd == "outage_on":

                for publisher in pubs.values():
                    publisher.set_outage(True)

                log.warning(
                    "Simulated application-level network outage enabled."
                )

            elif cmd == "outage_off":

                for publisher in pubs.values():
                    publisher.set_outage(False)

                log.info(
                    "Simulated application-level network outage disabled."
                )

            elif cmd == "reset":

                sim.reset_scenario()

                log.info("Scenario reset.")

            elif cmd == "status":

                log.info("SIM %s", sim.status())

                for n in nodes:

                    log.info(
                        "NODE %s state=%s buffer_depth=%d",
                        n,
                        pubs[n].state,
                        buf.count_unsynced(n)
                    )

            elif cmd == "quit":

                log.info("Quit command received.")
                return False

            else:

                log.warning(
                    "Unknown command: %s",
                    cmd
                )

        except (ValueError, KeyError) as exc:

            log.error(
                "Command failed: %s",
                exc
            )

        except Exception:

            log.exception(
                "Unexpected error while executing command: %s",
                line
            )

        return True

    # ---------------------------------------------------------
    # MAIN ACQUISITION LOOP
    # ---------------------------------------------------------

    next_tick = time.monotonic()
    next_depth = time.monotonic()
    alive = True

    try:

        while alive:

            elapsed = time.monotonic() - t_start

            # Process timed scenario commands
            while script and script[0][0] <= elapsed:

                _, command = script.pop(0)

                alive = handle(command) and alive

                if not alive:
                    break

            if not alive:
                break

            # Process terminal commands safely
            while True:

                try:
                    command = cmds.get_nowait()

                except queue.Empty:
                    break

                alive = handle(command) and alive

                if not alive:
                    break

            if not alive:
                break

            # Duration limit
            if args.duration is not None and elapsed >= args.duration:
                log.info("Duration limit reached.")
                break

            # Acquire readings
            if running and time.monotonic() >= next_tick:

                for n in nodes:

                    reading = sim.next_reading(n)

                    # Database first, network second
                    buf.insert_reading(reading)

                    rec.generated(
                        n,
                        reading["ts"]
                    )

                next_tick += args.tick

            # Record buffer depth
            if time.monotonic() >= next_depth:

                for n in nodes:

                    rec.depth(
                        n,
                        buf.count_unsynced(n)
                    )

                next_depth += 1.0

            time.sleep(0.02)

    except KeyboardInterrupt:

        log.info("Keyboard interrupt received.")

    except Exception:

        log.exception("Fatal error in main acquisition loop.")

    # ---------------------------------------------------------
    # SHUTDOWN
    # ---------------------------------------------------------

    log.info(
        "Stopping: waiting up to 20 seconds for the buffer to drain."
    )

    running = False

    end = time.monotonic() + 20

    while (
        time.monotonic() < end
        and buf.count_unsynced() > 0
        and all(
            publisher.state != "AUTH_FAILED"
            for publisher in pubs.values()
        )
    ):

        time.sleep(0.2)

    # Stop workers
    for worker in workers:
        worker.stop()

    # Wait for workers
    for worker in workers:
        worker.join(timeout=3)

    log.info(
        "Final buffer depth: %d unsynced rows",
        buf.count_unsynced()
    )

    log.info("Runner shutdown complete.")


if __name__ == "__main__":
    main()
