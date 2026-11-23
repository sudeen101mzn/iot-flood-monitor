"""Demo runner (part D): acquisition loop + one sync worker per node + scenario commands.

Commands (typed in the terminal, or timed in a scenario script):
  start | trigger_intrusion [node] | outage_on | outage_off | status |
  supply_interruption_on [node] | supply_interruption_off [node] | reset | quit

Acquisition never waits for the network: every reading goes into the SQLite buffer first,
and the per-node SyncWorker threads publish from there.
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

from sim import WaterQualitySimulator        # Somili's simulator
try:                                    # Sabin's real buffer when present, else the placeholder
    from buffer import Buffer
except ImportError:
    from buffer.stub_buffer import Buffer



def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--nodes", help="comma-separated node ids (default: all nodes in the scenario file)")
    p.add_argument("--scenario-file", default="scenarios/demo_scenario.json", help="simulator configuration (Somili)")
    p.add_argument("--host"), p.add_argument("--port", type=int)
    p.add_argument("--no-tls", action="store_true", help="plain MQTT (local testing only)")
    p.add_argument("--ca-file", help="CA certificate (needed only for a self-signed test broker)")
    p.add_argument("--tick", type=float, default=1.0, help="seconds between readings")
    p.add_argument("--time-scale", type=float, help="override the scenario time_scale (1 real s = N simulated s)")
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--stamp-mode", choices=["auto", "client"], default="auto",
                   help="auto: live=server-stamped, replay=client-stamped; client: always client-stamped")
    p.add_argument("--backoff-min", type=float, default=1.0)
    p.add_argument("--backoff-max", type=float, default=30.0)
    p.add_argument("--db", default="buffer.db")
    p.add_argument("--max-rows", type=int, default=10000)
    p.add_argument("--script", help="scenario file: lines like '30 trigger_intrusion' (seconds since start)")
    p.add_argument("--autostart", action="store_true")
    p.add_argument("--duration", type=float, help="stop after N seconds (headless runs)")
    p.add_argument("--no-stdin", action="store_true")
    p.add_argument("--log-dir", default="logs")
    return p.parse_args()


def load_script(path):
    steps = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.split("#")[0].strip()
        if line:
            t, cmd = line.split(None, 1)
            steps.append((float(t), cmd.strip()))
    return sorted(steps)


def stdin_reader(q):
    for line in sys.stdin:
        if line.strip():
            q.put(line.strip())


def main():
    args = parse_args()
    config.load_env()
    import os
    host = args.host or os.environ.get("TB_HOST", "demo.thingsboard.io")
    port = args.port or int(os.environ.get("TB_PORT", "8883"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s",
                        datefmt="%H:%M:%S")
    log = logging.getLogger("runner")
    rec = MetricsRecorder(args.log_dir)
    sim = WaterQualitySimulator(scenario_path=args.scenario_file)
    if args.time_scale:
        sim.set_time_scale(args.time_scale)
    nodes = [n.strip() for n in args.nodes.split(",")] if args.nodes else list(sim.nodes)
    buf = Buffer(args.db, max_rows=args.max_rows)
    pubs, workers = {}, []
    for n in nodes:
        token = config.token_for(n)
        if not token:
            log.warning("no token for %s (set TB_TOKEN_%s in .env); using a dummy", n, n.replace("-", "_"))
        pub = TbPublisher(n, token or "dummy", host, port, tls=not args.no_tls, ca_file=args.ca_file,
                          backoff=Backoff(args.backoff_min, args.backoff_max), recorder=rec)
        pubs[n] = pub
        w = SyncWorker(buf, pub, n, args.batch_size, args.stamp_mode, recorder=rec)
        w.start()
        workers.append(w)

    cmds = queue.Queue()
    if not args.no_stdin:
        threading.Thread(target=stdin_reader, args=(cmds,), daemon=True).start()
    script = load_script(args.script) if args.script else []
    running = args.autostart
    t_start = time.monotonic()
    log.info("runner ready: nodes=%s host=%s:%d tick=%.1fs time_scale=%.0f. Type 'start' to begin.",
             nodes, host, port, args.tick, sim.time_scale)

    def handle(line):
        nonlocal running
        parts = line.split()
        cmd, arg = parts[0], (parts[1] if len(parts) > 1 else None)
        log.info("COMMAND %s", line)
        rec.event("runner", "COMMAND", line)
        if cmd == "start":
            running = True
        elif cmd == "trigger_intrusion":
            sim.trigger_intrusion(arg)
        elif cmd == "supply_interruption_on":
            sim.set_supply_interruption(True, arg)
        elif cmd == "supply_interruption_off":
            sim.set_supply_interruption(False, arg)
        elif cmd == "outage_on":
            [p.set_outage(True) for p in pubs.values()]
        elif cmd == "outage_off":
            [p.set_outage(False) for p in pubs.values()]
        elif cmd == "reset":
            sim.reset_scenario()
        elif cmd == "status":
            log.info("SIM %s", sim.status())
            for n in nodes:
                log.info("NODE %s state=%s buffer_depth=%d", n, pubs[n].state, buf.count_unsynced(n))
        elif cmd == "quit":
            return False
        else:
            log.warning("unknown command: %s", cmd)
        return True

    next_tick, alive, next_depth = time.monotonic(), True, time.monotonic()
    try:
        while alive:
            elapsed = time.monotonic() - t_start
            while script and script[0][0] <= elapsed:
                alive = handle(script.pop(0)[1]) and alive
            while not cmds.empty():
                alive = handle(cmds.get()) and alive
            if args.duration and elapsed >= args.duration:
                break
            if running and time.monotonic() >= next_tick:
                for n in nodes:
                    r = sim.next_reading(n)
                    buf.insert_reading(r)              # database first, network second
                    rec.generated(n, r["ts"])
                next_tick += args.tick
            if time.monotonic() >= next_depth:
                for n in nodes:
                    rec.depth(n, buf.count_unsynced(n))
                next_depth += 1.0
            time.sleep(0.02)
    except KeyboardInterrupt:
        pass
    log.info("stopping: waiting up to 20 s for the buffer to drain")
    running = False
    end = time.monotonic() + 20
    while time.monotonic() < end and buf.count_unsynced() > 0 and all(p.state != "AUTH_FAILED" for p in pubs.values()):
        time.sleep(0.2)
    for w in workers:
        w.stop()
    for w in workers:
        w.join(timeout=3)
    log.info("final buffer depth: %d unsynced rows", buf.count_unsynced())


if __name__ == "__main__":
    main()
