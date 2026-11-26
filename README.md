# Post-flood water quality monitor: Task 3 (ITS67404)

Virtual node -> SQLite buffer -> MQTT over TLS (8883, QoS 1) -> ThingsBoard.

## Layout
| Folder | Owner | Content |
|---|---|---|
| `sim/` | Somili | `simulator.py`: coupled telemetry, intrusion model, supply interruption (config: `scenarios/demo_scenario.json`) |
| `buffer/` | Sabin | SQLite store-and-forward (`stub_buffer.py` is a placeholder) |
| `transport/` | Sudin | `publisher.py` (TLS, QoS 1, failure classification, backoff), `drain.py` (sync loop, joint with Sabin) |
| `metrics/`, `analysis/` | Sudin | CSV recorder, result table, plots, ThingsBoard REST helpers |
| `scenarios/` | Sudin | Timed demo scripts |
| `tools/`, `tests/` | Sudin | TLS smoke test, batch-size experiment, local broker test |

## Setup (Windows PowerShell)
```
python -m venv .venv
.\.venv\Scripts\Activate.ps1        # if blocked: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
pip install -r requirements.txt
copy .env.example .env              # then paste the three device tokens into .env (never commit it)
```

## Contract between the parts
* Simulator: `next_reading(node_id)` -> dict with `ts` (epoch ms), `node_id`, `ph`, `turbidity_ntu`, `tds_ppm`, `water_temp_c`,
  `flow_lpm`, `latitude`, `longitude`; also `trigger_intrusion(node_id=None)`, `set_supply_interruption(on, node_id=None)`,
  `set_time_scale(x)`, `status()`.
* Buffer: `insert_reading(row)`, `fetch_unsynced(limit, node_id=None)` (oldest first), `mark_synced(ids)`, `count_unsynced(node_id=None)`.
  Rows are marked synced ONLY after the QoS 1 PUBACK.
* The runner uses `sim.WaterQualitySimulator` (Somili) and `buffer.Buffer` if Sabin's real module is present, else the placeholder in `buffer/stub_buffer.py`.
* Tokens: one per node in `.env` (`TB_TOKEN_WQ_BID_001`, `_002`, `_018`). Demo speed: `--time-scale 20` gives a visible Warning -> Critical escalation (about 4 s and 10 s after `trigger_intrusion`); the scenario default of 60 gives about 1.3 s and 3.5 s.

## Run
1. Smoke test one device: `$env:TB_TOKEN="..."; python tools/tls_smoke_test.py`
2. Demo (interactive): `python run_demo.py`, then type `start`, `trigger_intrusion`, `outage_on`, `outage_off`, `status`,
   `supply_interruption_on` / `supply_interruption_off`, `reset`, `quit`.
3. Repeatable demo: `python run_demo.py --autostart --script scenarios/demo_timeline.txt` (commands in time order; the simulator config is `scenarios/demo_scenario.json`)
4. Results: `python analysis/report.py` (local) and `python analysis/report.py --rest --device WQ-BID-001` (needs `TB_USER`, `TB_PASSWORD`)
5. Batch size: `python tools/batch_experiment.py --token <TEST device token>` (writes lots of historic points: use a spare device)

## Tests (no ThingsBoard needed)
`python tests/run_local_test.py` starts a local TLS broker and checks: zero data loss through an outage, chronological replay with
original timestamps, reconnect with backoff, and that an authentication failure is NOT retried like a network failure.

## Status
Tested locally against a TLS test broker. NOT yet tested against demo.thingsboard.io; `analysis/tb_rest.py` is untested.
