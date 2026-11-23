"""Turn logs/*.csv into the mandatory result table and plots (matplotlib).

Local metrics (no server needed): delivery ratio, flush throughput, recovery time, PUBACK latency.
Server metrics (latency, alarm delay, data loss) need analysis/tb_rest.py and a tenant login:
    python analysis/report.py --rest --device WQ-BID-001
"""
import argparse
import csv
import statistics as st
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def pct(values, p):
    v = sorted(values)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))] if v else float("nan")


def summarise(name, v):
    return (f"{name}: mean={st.mean(v):.1f} median={st.median(v):.1f} P95={pct(v, 95):.1f} max={max(v):.1f} (n={len(v)})"
            if v else f"{name}: no data")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", default="logs")
    ap.add_argument("--rest", action="store_true")
    ap.add_argument("--device", default="WQ-BID-001")
    a = ap.parse_args()
    d = Path(a.logs)
    msgs, flush, gen = rows(d / "messages.csv"), rows(d / "flush.csv"), rows(d / "generated.csv")
    events, depth = rows(d / "events.csv"), rows(d / "depth.csv")

    live = [m for m in msgs if m["mode"] == "live" and m["ok"] == "1"]
    ack_ms = [int(m["puback_ms"]) - int(m["publish_ms"]) for m in live]
    delivered_rows = sum(int(m["batch_size"]) for m in msgs if m["ok"] == "1")
    print("== local results ==")
    print(summarise("PUBACK round-trip ms (live messages)", ack_ms))
    print(f"messages generated={len(gen)}  rows acknowledged (incl. any re-sends)={delivered_rows}")
    for f in flush:
        print(f"flush {f['node']}: {f['rows']} rows in {f['seconds']} s = {f['rows_per_s']} rows/s, "
              f"backlog {f['backlog_at_start']}, recovery {f['recovery_s']} s after outage_off")

    if a.rest:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from tb_rest import TbRest
        tb = TbRest()
        dev = tb.device_id(a.device)
        gts = [int(g["ts_ms"]) for g in gen if g["node"] == a.device]
        data = tb.timeseries(dev, ["gen_ts_ms", "ph"], min(gts) - 5000, max(gts) + 120000)
        lat = [p["ts"] - int(float(p["value"])) for p in data.get("gen_ts_ms", [])]
        print("== server results ==")
        print(summarise("end-to-end latency ms (server ts - gen_ts_ms)", lat))
        present = len(data.get("ph", []))
        print(f"data loss: generated={len(gts)} present={present} lost={max(0, len(gts) - present)}")

    # plot 1: buffer depth over time, outage window shaded
    t0 = min(int(r["wall_ms"]) for r in depth) if depth else 0
    fig, ax = plt.subplots(figsize=(6.5, 3))
    for node in sorted({r["node"] for r in depth}):
        pts = [(int(r["wall_ms"]) - t0) / 1000 for r in depth if r["node"] == node], \
              [int(r["unsynced"]) for r in depth if r["node"] == node]
        ax.plot(*pts, label=node)
    on = [(int(e["wall_ms"]) - t0) / 1000 for e in events if e["event"] == "OUTAGE_ON"]
    off = [(int(e["wall_ms"]) - t0) / 1000 for e in events if e["event"] == "OUTAGE_OFF"]
    if on and off:
        ax.axvspan(on[0], off[0], color="red", alpha=0.12, label="outage")
    ax.set_xlabel("time since start (s)"); ax.set_ylabel("unsynced rows"); ax.legend(fontsize=7); ax.grid(alpha=.3)
    fig.tight_layout(); fig.savefig(d / "fig_buffer_depth.png", dpi=300)
    # plot 2: PUBACK round-trip histogram
    if ack_ms:
        fig, ax = plt.subplots(figsize=(6.5, 3))
        ax.hist(ack_ms, bins=30)
        ax.set_xlabel("QoS 1 acknowledgement time (ms)"); ax.set_ylabel("messages"); ax.grid(alpha=.3)
        fig.tight_layout(); fig.savefig(d / "fig_ack_latency.png", dpi=300)
    print("wrote", d / "fig_buffer_depth.png", d / "fig_ack_latency.png")


if __name__ == "__main__":
    main()
