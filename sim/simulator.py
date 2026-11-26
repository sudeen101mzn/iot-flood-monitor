"""Virtual edge telemetry simulator for post-flood water quality monitoring."""

from __future__ import annotations

import json
import math
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class NodeState:
    """Tracks per-node site attributes, baseline values, events, and alarm hysteresis."""

    node_id: str
    label: str
    latitude: float
    longitude: float
    ph_base: float = 7.20
    turbidity_base: float = 4.0
    tds_base: int = 180
    flow_base: float = 13.0
    supply_interrupted: bool = False
    zero_flow_samples: int = 0
    intrusion_start_sim_sec: float | None = None
    intrusion_count: int = 0
    active_alarms: set[str] = field(default_factory=set)
    extra: dict[str, Any] = field(default_factory=dict)


class WaterQualitySimulator:
    """Generates physically coupled water quality readings across multiple nodes."""

    def __init__(
        self,
        scenario_path: str | Path | None = None,
        seed: int | None = 42,
        sustained_zero_flow_samples: int = 2,
    ) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        self.sustained_zero_flow_samples = max(1, int(sustained_zero_flow_samples))
        self._real_start = time.monotonic()
        self._last_real_tick = self._real_start
        self.sim_elapsed_sec: float = 0.0
        self.time_scale: float = 60.0

        cfg = self._load_scenario(scenario_path)
        self.scenario_name: str = cfg.get("name", "default_scenario")
        self.time_scale = float(cfg.get("time_scale", self.time_scale))
        self.default_intrusion_node: str = cfg.get(
            "default_intrusion_node", "WQ-BID-001"
        )
        self.default_interruption_node: str = cfg.get(
            "default_interruption_node", "WQ-BID-018"
        )

        model = cfg.get("intrusion_model", {})
        self.rise_sec: float = float(model.get("rise_minutes", 5.0)) * 60.0
        self.washout_tau_sec: float = float(model.get("washout_tau_minutes", 25.0)) * 60.0
        self.turbidity_spike_ntu: float = float(model.get("turbidity_spike_ntu", 340.0))
        self.tds_gain_per_ntu: float = float(model.get("tds_gain_per_ntu", 1.35))
        self.ph_drop_max: float = float(model.get("ph_drop_max", 1.35))
        self.ph_floor: float = float(model.get("ph_floor", 5.80))
        self.flow_transient_amp: float = float(model.get("flow_transient_amp", 3.5))

        self.nodes: dict[str, NodeState] = {}
        for nid, nd in cfg.get("nodes", {}).items():
            self.nodes[nid] = NodeState(
                node_id=nid,
                label=nd.get("label", nid),
                latitude=float(nd["latitude"]),
                longitude=float(nd["longitude"]),
                ph_base=float(nd.get("ph_base", 7.20)),
                turbidity_base=float(nd.get("turbidity_base", 4.0)),
                tds_base=int(nd.get("tds_base", 180)),
                flow_base=float(nd.get("flow_base", 13.0)),
            )

        if not self.nodes:
            self._init_default_nodes()

    @staticmethod
    def _load_scenario(scenario_path: str | Path | None) -> dict[str, Any]:
        if scenario_path is None:
            return {}
        path = Path(scenario_path)
        if not path.exists():
            raise FileNotFoundError(f"Scenario file not found: {path}")
        with path.open("r", encoding="utf-8-sig") as f:
            return json.load(f)

    def _init_default_nodes(self) -> None:
        defaults = [
            ("WQ-BID-001", "Holding-Centre Water Point", 27.9115, 85.1462, 7.20, 4.5, 180, 14.0),
            ("WQ-BID-002", "Community Tap", 27.9158, 85.1510, 7.15, 3.8, 165, 12.5),
            ("WQ-BID-018", "Reservoir Outlet", 27.9072, 85.1415, 7.25, 4.2, 175, 16.0),
        ]
        for nid, label, lat, lon, ph, turb, tds, flow in defaults:
            self.nodes[nid] = NodeState(
                node_id=nid,
                label=label,
                latitude=lat,
                longitude=lon,
                ph_base=ph,
                turbidity_base=turb,
                tds_base=tds,
                flow_base=flow,
            )

    def reset_scenario(self) -> None:
        """Reset simulated clock, active intrusions, supply cuts, and alarm states."""
        self.rng = random.Random(self.seed)
        self._real_start = time.monotonic()
        self._last_real_tick = self._real_start
        self.sim_elapsed_sec = 0.0
        for node in self.nodes.values():
            node.supply_interrupted = False
            node.zero_flow_samples = 0
            node.intrusion_start_sim_sec = None
            node.intrusion_count = 0
            node.active_alarms.clear()

    def _advance_clock(self) -> None:
        now = time.monotonic()
        delta_real = max(0.0, now - self._last_real_tick)
        self._last_real_tick = now
        self.sim_elapsed_sec += delta_real * self.time_scale

    def set_time_scale(self, scale: float) -> float:
        """Update simulated time multiplier (e.g. 60.0 = 1 sim min per real sec)."""
        if scale <= 0:
            raise ValueError("time_scale must be greater than 0")
        self._advance_clock()
        self.time_scale = float(scale)
        return self.time_scale

    def trigger_intrusion(self, node_id: str | None = None) -> str:
        """Trigger a rainfall-driven silt/sewage intrusion event on a node."""
        self._advance_clock()
        target = node_id or self.default_intrusion_node
        if target not in self.nodes:
            raise KeyError(f"Unknown node_id: {target}")
        node = self.nodes[target]
        node.intrusion_start_sim_sec = self.sim_elapsed_sec
        node.intrusion_count += 1
        return target

    def set_supply_interruption(self, active: bool, node_id: str | None = None) -> str:
        """Toggle supply interruption (flow_lpm = 0.0) on a target node."""
        target = node_id or self.default_interruption_node
        if target not in self.nodes:
            raise KeyError(f"Unknown node_id: {target}")
        node = self.nodes[target]
        node.supply_interrupted = bool(active)
        if not node.supply_interrupted:
            node.zero_flow_samples = 0
        return target

    def _intrusion_envelope(self, node: NodeState) -> tuple[float, float]:
        """Return (intensity in [0, 1], pressure_transient) for the node."""
        if node.intrusion_start_sim_sec is None:
            return 0.0, 0.0
        dt = self.sim_elapsed_sec - node.intrusion_start_sim_sec
        if dt < 0:
            return 0.0, 0.0

        if dt <= self.rise_sec:
            rise_frac = dt / max(self.rise_sec, 1.0)
            intensity = rise_frac ** 1.5
            transient = self.flow_transient_amp * math.sin(math.pi * rise_frac)
        else:
            wash_dt = dt - self.rise_sec
            decay = math.exp(-wash_dt / max(self.washout_tau_sec, 1.0))
            intensity = decay
            transient = -0.5 * self.flow_transient_amp * math.exp(-wash_dt / 120.0)

        return max(0.0, min(1.0, intensity)), transient

    def _update_hysteresis_alarms(self, node: NodeState, reading: dict[str, Any]) -> list[str]:
        """Evaluate create/clear hysteresis rules for local logging/demo visibility."""
        turb = reading["turbidity_ntu"]
        ph = reading["ph"]
        tds = reading["tds_ppm"]
        flow = reading["flow_lpm"]

        if flow <= 0.0:
            node.zero_flow_samples += 1
        else:
            node.zero_flow_samples = 0

        rules: list[tuple[str, bool, bool]] = [
            ("HIGH_TURBIDITY", turb > 50.0, turb < 40.0),
            ("GROSS_CONTAMINATION", turb > 200.0, turb < 150.0),
            ("PH_OUT_OF_RANGE", (ph < 6.5) or (ph > 8.5), (6.7 <= ph <= 8.3)),
            ("HIGH_TDS", tds > 500, tds < 450),
            (
                "SUPPLY_INTERRUPTION",
                node.zero_flow_samples >= self.sustained_zero_flow_samples,
                flow > 0.0,
            ),
        ]

        transitions: list[str] = []
        for alarm_name, create_cond, clear_cond in rules:
            is_active = alarm_name in node.active_alarms
            if not is_active and create_cond:
                node.active_alarms.add(alarm_name)
                transitions.append(f"+{alarm_name}")
            elif is_active and clear_cond:
                node.active_alarms.remove(alarm_name)
                transitions.append(f"-{alarm_name}")

        return transitions

    def next_reading(self, node_id: str) -> dict[str, Any]:
        """Generate one telemetry reading dict with Unix epoch ms timestamp."""
        if node_id not in self.nodes:
            raise KeyError(f"Unknown node_id: {node_id}")

        self._advance_clock()
        node = self.nodes[node_id]
        ts_ms = int(time.time() * 1000)

        sim_hours = (self.sim_elapsed_sec / 3600.0) % 24.0
        diurnal_norm = 0.5 * (1.0 - math.cos(2.0 * math.pi * (sim_hours - 3.0) / 24.0))
        temp_c = 16.0 + 8.0 * diurnal_norm + self.rng.gauss(0.0, 0.25)
        temp_c = max(15.0, min(25.0, temp_c))

        intensity, flow_transient = self._intrusion_envelope(node)

        turbidity = (
            node.turbidity_base
            + self.turbidity_spike_ntu * intensity
            + self.rng.gauss(0.0, 0.4 + 0.04 * self.turbidity_spike_ntu * intensity)
        )
        turbidity = max(0.5, min(850.0, turbidity))
        turbidity_added = max(0.0, turbidity - node.turbidity_base)

        tds = (
            node.tds_base
            + self.tds_gain_per_ntu * turbidity_added
            + self.rng.gauss(0.0, 4.0)
        )
        tds_int = max(50, int(round(tds)))

        ph_drop = self.ph_drop_max * (intensity ** 1.3)
        ph = node.ph_base - ph_drop + self.rng.gauss(0.0, 0.04)
        ph = max(self.ph_floor, min(8.8, ph))

        if node.supply_interrupted:
            flow = 0.0
        else:
            flow = node.flow_base + flow_transient + self.rng.gauss(0.0, 0.35)
            flow = max(0.2, flow)

        reading = {
            "ts": ts_ms,
            "node_id": node.node_id,
            "ph": round(ph, 2),
            "turbidity_ntu": round(turbidity, 1),
            "tds_ppm": tds_int,
            "water_temp_c": round(temp_c, 1),
            "flow_lpm": round(flow, 1),
            "latitude": node.latitude,
            "longitude": node.longitude,
        }

        self._update_hysteresis_alarms(node, reading)
        return reading

    def get_alarms(self, node_id: str) -> list[str]:
        """Return currently active local hysteresis alarms for a node."""
        if node_id not in self.nodes:
            raise KeyError(f"Unknown node_id: {node_id}")
        return sorted(self.nodes[node_id].active_alarms)

    def status(self) -> dict[str, Any]:
        """Return current simulator clock and per-node state summary."""
        self._advance_clock()
        return {
            "scenario_name": self.scenario_name,
            "sim_elapsed_sec": round(self.sim_elapsed_sec, 1),
            "time_scale": self.time_scale,
            "nodes": {
                nid: {
                    "label": n.label,
                    "coords": [n.latitude, n.longitude],
                    "intrusion_active": n.intrusion_start_sim_sec is not None,
                    "intrusion_count": n.intrusion_count,
                    "supply_interrupted": n.supply_interrupted,
                    "active_alarms": sorted(n.active_alarms),
                }
                for nid, n in self.nodes.items()
            },
        }