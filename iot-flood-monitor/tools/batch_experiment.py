"""Find the best replay batch size empirically (the brief asks for it).

For each batch size it loads N old readings into a fresh buffer and times the drain over the real
connection. WARNING: this writes N x len(sizes) historic data points to the device you point it
at, so use a dedicated TEST device (not one of the three dashboard nodes).

    python tools/batch_experiment.py --token <test-device-token> --rows 1000
Local check:  python tools/batch_experiment.py --host localhost --ca-file certs/cert.pem --token x
"""
import argparse
import csv
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from buffer.stub_buffer import Buffer
from transport import Backoff, TbPublisher
from transport.drain import drain


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="demo.thingsboard.io")
    ap.add_argument("--port", type=int, default=8883)
    ap.add_argument("--token", required=True)
    ap.add_argument("--ca-file")
    ap.add_argument("--rows", type=int, default=1000)
    ap.add_argument("--sizes", default="1,5,10,25,50,100,200")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--out", default="logs")
    a = ap.parse_args()
    sizes = [int(s) for s in a.sizes.split(",")]
    Path(a.out).mkdir(exist_ok=True)
    now_ms = int(time.time() * 1000)
    results, block = [], 0
    for size in sizes:
        rates = []
        for _ in range(a.repeats):
            block += 1
            base = now_ms - 3_600_000 - block * a.rows * 1000          # every run gets its own unique past window
            buf = Buffer(str(Path(tempfile.mkdtemp()) / "b.db"), max_rows=10 * a.rows)
            for i in range(a.rows):
                buf.insert_reading({"node_id": "TEST", "ts": base + i * 1000, "ph": 7.2, "turbidity_ntu": 5.0,
                                    "tds_ppm": 180, "water_temp_c": 20.0, "flow_lpm": 13.0,
                                    "latitude": 27.9, "longitude": 85.1})
            pub = TbPublisher("TEST", a.token, a.host, a.port, tls=True, ca_file=a.ca_file, backoff=Backoff(0.5, 2))
            pub.ensure_connected()                                    # connect before timing the drain
            t0 = time.perf_counter()
            st = drain(buf, pub, "TEST", batch_size=size, stamp_mode="client")
            dt = time.perf_counter() - t0
            pub.close()
            if st.rows != a.rows:
                print(f"batch {size}: INCOMPLETE ({st.rows}/{a.rows}, {st.result})")
                continue
            rates.append(a.rows / dt)
        if rates:
            mean = sum(rates) / len(rates)
            results.append((size, mean, min(rates), max(rates)))
            print(f"batch size {size:>4}: {mean:8.1f} rows/s  (min {min(rates):.1f}, max {max(rates):.1f}, runs {len(rates)})")
    with open(Path(a.out) / "batch_experiment.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["batch_size", "mean_rows_per_s", "min", "max"]); w.writerows(results)
    if results:
        fig, ax = plt.subplots(figsize=(6.5, 3))
        ax.errorbar([r[0] for r in results], [r[1] for r in results],
                    yerr=[[r[1] - r[2] for r in results], [r[3] - r[1] for r in results]], marker="o", capsize=3)
        ax.set_xscale("log"); ax.set_xlabel("batch size (rows per publish)"); ax.set_ylabel("flush throughput (rows/s)")
        ax.grid(alpha=.3); fig.tight_layout(); fig.savefig(Path(a.out) / "fig_batch_size.png", dpi=300)
        print("best:", max(results, key=lambda r: r[1])[0], "| wrote", Path(a.out) / "fig_batch_size.png")


if __name__ == "__main__":
    main()
