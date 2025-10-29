import json
import time
import threading
from datetime import datetime
from typing import Optional, Dict

import psutil

# Try NVML for real GPU power; otherwise fall back to estimates.
NVML_AVAILABLE = False
try:
    # Prefer the maintained nvidia-ml-py; fall back to pynvml if present.
    try:
        import pynvml  # deprecated but widely installed
        pynvml.nvmlInit()
        NVML_AVAILABLE = True
        _NVML = pynvml
    except Exception:
        NVML_AVAILABLE = False
except Exception:
    NVML_AVAILABLE = False


# Simple region → carbon intensity lookup (gCO2/kWh).
DEFAULT_GRID_INTENSITY: Dict[str, int] = {
    "DE": 401,   # Germany (example average)
    "FR": 53,    # France
    "US": 386,   # USA average
    "CN": 556,   # China
    "GB": 199,   # Great Britain
    "IN": 708,   # India
    "EU": 275,   # EU27 average (illustrative)
}


def _gpu_metrics_nvml():
    """Return dict with gpu_power_w, gpu_utilization_pct, gpu_mem_used_mb using NVML."""
    try:
        handle = _NVML.nvmlDeviceGetHandleByIndex(0)
        power = _NVML.nvmlDeviceGetPowerUsage(handle) / 1000.0  # W
        util = _NVML.nvmlDeviceGetUtilizationRates(handle).gpu
        mem = _NVML.nvmlDeviceGetMemoryInfo(handle).used / (1024 * 1024)
        return {
            "gpu_power_w": round(power, 2),
            "gpu_utilization_pct": int(util),
            "gpu_mem_used_mb": round(mem, 2),
        }
    except Exception:
        # If NVML is present but fails, return Nones; caller may estimate from util if available.
        return {"gpu_power_w": None, "gpu_utilization_pct": None, "gpu_mem_used_mb": None}


def _gpu_metrics_estimated(gpu_utilization_pct: Optional[float], gpu_tdp_w: float):
    """Estimate GPU power from utilization and a nominal TDP."""
    if gpu_utilization_pct is None:
        return {"gpu_power_w": None}
    # Very simple linear estimate; you can refine later (idle/base power, nonlinear curve).
    est_power = max(0.0, min(100.0, float(gpu_utilization_pct))) / 100.0 * float(gpu_tdp_w)
    return {"gpu_power_w": round(est_power, 2)}


def _sample_once(
    run_id: str,
    user_id: str,
    model_name: str,
    region_iso: str,
    cpu_tdp_w: float,
    gpu_tdp_w: float,
    prefer_nvml: bool = True,
):
    """Collect one sample of system metrics and return a dict (without energy/emissions)."""
    vm = psutil.virtual_memory()
    net = psutil.net_io_counters()
    cpu_util = psutil.cpu_percent(interval=None)

    # Start with no GPU info; fill from NVML or leave None.
    gpu_power_w = None
    gpu_util = None
    gpu_mem_mb = None

    if prefer_nvml and NVML_AVAILABLE:
        g = _gpu_metrics_nvml()
        gpu_power_w = g.get("gpu_power_w")
        gpu_util = g.get("gpu_utilization_pct")
        gpu_mem_mb = g.get("gpu_mem_used_mb")

    # If we don't have power but do have util, estimate power.
    if gpu_power_w is None and gpu_util is not None:
        gpu_power_w = _gpu_metrics_estimated(gpu_util, gpu_tdp_w)["gpu_power_w"]

    rec = {
        "timestamp": datetime.utcnow().isoformat(),
        "run_id": run_id,
        "user_id": user_id,
        "model_name": model_name,
        "region_iso": region_iso,
        # Raw system metrics
        "cpu_utilization_pct": round(float(cpu_util), 2),
        "ram_used_mb": round(vm.used / (1024 * 1024), 2),
        "bytes_sent": int(net.bytes_sent),
        "bytes_recv": int(net.bytes_recv),
        "gpu_utilization_pct": None if gpu_util is None else int(gpu_util),
        "gpu_mem_used_mb": None if gpu_mem_mb is None else round(gpu_mem_mb, 2),
        # Instantaneous power (W); cpu will be estimated below
        "gpu_power_w": None if gpu_power_w is None else round(float(gpu_power_w), 2),
    }

    # Estimate CPU power from utilization × TDP (simple proportional model).
    rec["cpu_power_w"] = round((rec["cpu_utilization_pct"] / 100.0) * float(cpu_tdp_w), 2)

    # If GPU power is still None and we have no util, estimate from TDP at a low baseline (10%).
    if rec["gpu_power_w"] is None:
        rec["gpu_power_w"] = round(0.1 * float(gpu_tdp_w), 2)

    return rec


class MetricsCollector:
    """
    Context-managed collector that samples metrics every `interval_s`,
    estimates energy and emissions, and writes JSON lines to `output_file`.
    """

    def __init__(
        self,
        output_file: str,
        run_id: str = "run_001",
        user_id: str = "user",
        model_name: str = "model",
        region: str = "DE",
        interval_s: float = 2.0,
        # Estimation parameters
        cpu_tdp_w: float = 65.0,
        gpu_tdp_w: float = 200.0,
        grid_intensity_g_per_kwh: Optional[float] = None,
        prefer_nvml: bool = True,
    ):
        self.output_file = output_file
        self.run_id = run_id
        self.user_id = user_id
        self.model_name = model_name
        self.region = region
        self.interval_s = float(interval_s)
        self.cpu_tdp_w = float(cpu_tdp_w)
        self.gpu_tdp_w = float(gpu_tdp_w)
        self.prefer_nvml = prefer_nvml

        # If no explicit intensity provided, use region lookup with fallback 400 g/kWh.
        self.grid_intensity_g_per_kwh = (
            float(grid_intensity_g_per_kwh)
            if grid_intensity_g_per_kwh is not None
            else float(DEFAULT_GRID_INTENSITY.get(region, 400))
        )

        # Runtime state
        self._stop = threading.Event()
        self._t: Optional[threading.Thread] = None
        self._f = None
        self._last_t = None  # monotonic timestamp of last sample
        self.cum_energy_kwh = 0.0
        self.cum_emissions_kg = 0.0

    def _loop(self):
        self._last_t = time.monotonic()
        while not self._stop.is_set():
            now = time.monotonic()
            delta_s = max(0.0, now - self._last_t)
            self._last_t = now

            rec = _sample_once(
                run_id=self.run_id,
                user_id=self.user_id,
                model_name=self.model_name,
                region_iso=self.region,
                cpu_tdp_w=self.cpu_tdp_w,
                gpu_tdp_w=self.gpu_tdp_w,
                prefer_nvml=self.prefer_nvml,
            )

            # Compute energy and emissions for this interval.
            power_total_w = float(rec["cpu_power_w"]) + float(rec["gpu_power_w"] or 0.0)
            energy_kwh_interval = (power_total_w * delta_s) / 3_600_000.0  # W*s → kWh
            emissions_kg_interval = (
                energy_kwh_interval * self.grid_intensity_g_per_kwh / 1000.0
            )

            self.cum_energy_kwh += energy_kwh_interval
            self.cum_emissions_kg += emissions_kg_interval

            rec.update(
                {
                    "delta_seconds": round(delta_s, 3),
                    "power_total_w": round(power_total_w, 2),
                    "energy_kwh_interval": round(energy_kwh_interval, 9),
                    "emissions_kg_interval": round(emissions_kg_interval, 9),
                    "cumulative_energy_kwh": round(self.cum_energy_kwh, 9),
                    "cumulative_emissions_kg": round(self.cum_emissions_kg, 9),
                    "grid_carbon_intensity_g_per_kwh": self.grid_intensity_g_per_kwh,
                }
            )

            json.dump(rec, self._f)
            self._f.write("\n")
            self._f.flush()

            # Sleep the remainder of the interval (if any).
            to_sleep = max(0.0, self.interval_s - (time.monotonic() - now))
            time.sleep(to_sleep)

    def __enter__(self):
        self._f = open(self.output_file, "a")
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()
        print(
            f"Started metrics collection (interval={self.interval_s}s, "
            f"grid_intensity={self.grid_intensity_g_per_kwh} g/kWh)"
        )
        return self

    def __exit__(self, exc_type, exc, tb):
        self._stop.set()
        if self._t:
            self._t.join()
        if self._f:
            self._f.close()
        if NVML_AVAILABLE:
            try:
                _NVML.nvmlShutdown()
            except Exception:
                pass
        print(
            f"Stopped metrics collection. Totals: "
            f"{self.cum_energy_kwh:.6f} kWh, {self.cum_emissions_kg:.6f} kg CO2."
        )