import json
import time
import threading
import traceback
import platform
from datetime import datetime
from typing import Optional, Dict, Any

import psutil
from kafka.producer import KafkaProducerWrapper

# -----------------------------
# Optional NVML support for real GPU metrics
# -----------------------------
NVML_AVAILABLE = False
try:
    try:
        import pynvml as _NVML  # type: ignore
        _NVML.nvmlInit()
        NVML_AVAILABLE = True
    except Exception:
        _NVML = None  # type: ignore
        NVML_AVAILABLE = False
except Exception:
    _NVML = None  # type: ignore
    NVML_AVAILABLE = False


# -----------------------------
# Default grid carbon intensity (g CO2 per kWh)
# -----------------------------
DEFAULT_GRID_INTENSITY_G_PER_KWH: Dict[str, int] = {
    "DE": 400,   # Germany average
    "FR": 53,    # France
    "US": 386,   # USA average
    "CN": 556,   # China
    "GB": 199,   # Great Britain
    "IN": 708,   # India
    "EU": 275,   # EU27 average (illustrative)
}


def _gpu_metrics_nvml() -> Dict[str, Optional[float]]:
    """Return gpu_power_w, gpu_utilization_pct, gpu_mem_used_mb using NVML."""
    if not NVML_AVAILABLE or _NVML is None:
        return {
            "gpu_power_w": None,
            "gpu_utilization_pct": None,
            "gpu_mem_used_mb": None,
        }

    try:
        handle = _NVML.nvmlDeviceGetHandleByIndex(0)
        power = _NVML.nvmlDeviceGetPowerUsage(handle) / 1000.0  # W
        util = _NVML.nvmlDeviceGetUtilizationRates(handle).gpu
        mem_info = _NVML.nvmlDeviceGetMemoryInfo(handle)
        mem_mb = mem_info.used / (1024 * 1024)
        return {
            "gpu_power_w": float(power),
            "gpu_utilization_pct": float(util),
            "gpu_mem_used_mb": float(mem_mb),
        }
    except Exception:
        return {
            "gpu_power_w": None,
            "gpu_utilization_pct": None,
            "gpu_mem_used_mb": None,
        }


def _gpu_metrics_estimated(gpu_util_pct: float, gpu_tdp_w: float) -> Dict[str, float]:
    """
    Estimate GPU power based on utilization and TDP (very rough model).
    """
    util = max(0.0, min(100.0, gpu_util_pct))
    power = (util / 100.0) * float(gpu_tdp_w)
    return {
        "gpu_power_w": power,
    }


def _sample_once(
    run_id: str,
    user_id: str,
    model_name: str,
    region_iso: str,
    cpu_tdp_w: float,
    gpu_tdp_w: float,
    prefer_nvml: bool = True,
) -> Dict[str, Any]:
    """
    Collect a single sample of system metrics and instantaneous power.
    Energy/emissions are added later in the collector loop.
    """
    vm = psutil.virtual_memory()
    net = psutil.net_io_counters()
    cpu_util = psutil.cpu_percent(interval=None)

    # GPU metrics (optional)
    gpu_power_w: Optional[float] = None
    gpu_util: Optional[float] = None
    gpu_mem_mb: Optional[float] = None

    if prefer_nvml and NVML_AVAILABLE:
        g = _gpu_metrics_nvml()
        gpu_power_w = g.get("gpu_power_w")
        gpu_util = g.get("gpu_utilization_pct")
        gpu_mem_mb = g.get("gpu_mem_used_mb")

    if gpu_power_w is None and gpu_util is not None:
        gpu_power_w = _gpu_metrics_estimated(gpu_util, gpu_tdp_w)["gpu_power_w"]

    rec: Dict[str, Any] = {
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
        # Instantaneous GPU power (may be None)
        "gpu_power_w": None if gpu_power_w is None else round(float(gpu_power_w), 2),
    }

    # Estimate CPU power from utilization × TDP (simple proportional model).
    rec["cpu_power_w"] = round(
        (rec["cpu_utilization_pct"] / 100.0) * float(cpu_tdp_w),
        2,
    )

    # If GPU power is still None, fall back to a low baseline (10% of TDP).
    if rec["gpu_power_w"] is None:
        rec["gpu_power_w"] = round(0.1 * float(gpu_tdp_w), 2)

    return rec


class MetricsCollector:
    """
    Context-managed collector that samples metrics every `interval_s`,
    estimates energy and emissions, and sends records to Kafka.

    It also keeps track of training state (epoch, step, loss, accuracy)
    and run metadata (dataset, framework, hyperparameters, hardware, etc.).
    At the end of the run, a run summary is sent to a separate Kafka topic.
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
        # Training / run metadata
        dataset_name: str = "unknown_dataset",
        framework: str = "unknown_framework",
        hyperparameters: Optional[Dict[str, Any]] = None,
        environment: str = "local",
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
        self.kafka_producer = KafkaProducerWrapper()

        # Run metadata
        self.dataset_name = dataset_name
        self.framework = framework
        self.hyperparameters = hyperparameters or {}
        self.environment = environment

        # Grid intensity
        self.grid_intensity_g_per_kwh = (
            float(grid_intensity_g_per_kwh)
            if grid_intensity_g_per_kwh is not None
            else float(DEFAULT_GRID_INTENSITY_G_PER_KWH.get(region, 400))
        )

        # Runtime state
        self._stop = threading.Event()
        self._t: Optional[threading.Thread] = None
        self._f = None
        self._last_t: Optional[float] = None  # monotonic timestamp of last sample
        self.cum_energy_kwh = 0.0
        self.cum_emissions_kg = 0.0

        # Training state
        self.epoch: Optional[int] = None
        self.step: Optional[int] = None
        self.loss: Optional[float] = None
        self.accuracy: Optional[float] = None

        # Run status and timing
        self.status: str = "running"
        self.start_time: datetime = datetime.utcnow()

        # Static hardware info
        self.hardware_info = self._collect_hardware_info()

    # -----------------------------
    # Training state updates
    # -----------------------------
    def update_training_state(
        self,
        epoch: Optional[int] = None,
        step: Optional[int] = None,
        loss: Optional[float] = None,
        accuracy: Optional[float] = None,
    ) -> None:
        """Update the current training state that will be attached to each record."""
        if epoch is not None:
            self.epoch = int(epoch)
        if step is not None:
            self.step = int(step)
        if loss is not None:
            self.loss = float(loss)
        if accuracy is not None:
            self.accuracy = float(accuracy)

    # -----------------------------
    # Hardware summary
    # -----------------------------
    def _collect_hardware_info(self) -> Dict[str, Any]:
        """Collect static hardware information for the run summary and records."""
        info: Dict[str, Any] = {}

        # CPU
        info["cpu_model"] = platform.processor()
        info["cpu_cores"] = psutil.cpu_count(logical=False)
        info["cpu_threads"] = psutil.cpu_count(logical=True)

        # RAM
        try:
            info["total_ram_gb"] = round(psutil.virtual_memory().total / 1e9, 2)
        except Exception:
            info["total_ram_gb"] = None

        # GPU (optional)
        if NVML_AVAILABLE and _NVML is not None:
            try:
                handle = _NVML.nvmlDeviceGetHandleByIndex(0)
                name = _NVML.nvmlDeviceGetName(handle).decode()
                vram = _NVML.nvmlDeviceGetMemoryInfo(handle).total / 1e9
                info["gpu_name"] = name
                info["gpu_vram_gb"] = round(vram, 2)
            except Exception:
                info["gpu_name"] = None
                info["gpu_vram_gb"] = None
        else:
            info["gpu_name"] = None
            info["gpu_vram_gb"] = None

        return info

    # -----------------------------
    # Main sampling loop
    # -----------------------------
    def _loop(self) -> None:
        self._last_t = time.monotonic()
        while not self._stop.is_set():
            now = time.monotonic()
            delta_s = max(0.0, now - (self._last_t or now))
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
            energy_kwh_interval = (power_total_w * delta_s) / 3_600_000.0  # W*s -> kWh
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
                    # Run metadata
                    "dataset_name": self.dataset_name,
                    "framework": self.framework,
                    "environment": self.environment,
                    "hyperparameters": self.hyperparameters,
                    "hardware_info": self.hardware_info,
                    # Training state
                    "epoch": self.epoch,
                    "step": self.step,
                    "loss": self.loss,
                    "accuracy": self.accuracy,
                    # Status (at sampling time)
                    "status": self.status,
                }
            )

            # Append to local file (JSONL) if requested.
            if self.output_file:
                if self._f is not None:
                    json.dump(rec, self._f)
                    self._f.write("\n")
                    self._f.flush()

            # Send to Kafka metrics topic
            try:
                self.kafka_producer.produce(
                    topic="training.metrics",
                    value=json.dumps(rec).encode("utf-8"),
                )
            except Exception as e:
                print(f"Failed to send metrics to Kafka: {e}")

            # Sleep remaining time in interval
            to_sleep = max(0.0, self.interval_s - (time.monotonic() - now))
            time.sleep(to_sleep)

    # -----------------------------
    # Context manager methods
    # -----------------------------
    def __enter__(self) -> "MetricsCollector":
        if self.output_file:
            self._f = open(self.output_file, "a")
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()
        print(
            f"Started metrics collection for run_id={self.run_id}, "
            f"model_name={self.model_name}, region={self.region}"
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        # Mark status based on exception
        if exc_type is not None:
            self.status = "aborted"
            print("Training aborted with exception:")
            traceback.print_exception(exc_type, exc, tb)
        else:
            self.status = "completed"

        # Stop background thread
        self._stop.set()
        if self._t:
            self._t.join()
        if self._f:
            self._f.close()

        # Shutdown NVML if used
        if NVML_AVAILABLE and _NVML is not None:
            try:
                _NVML.nvmlShutdown()
            except Exception:
                pass

        # Send run summary to Kafka
        end_time = datetime.utcnow()
        summary_record: Dict[str, Any] = {
            "run_id": self.run_id,
            "user_id": self.user_id,
            "model_name": self.model_name,
            "dataset_name": self.dataset_name,
            "framework": self.framework,
            "environment": self.environment,
            "region_iso": self.region,
            "hyperparameters": self.hyperparameters,
            "hardware_info": self.hardware_info,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": (end_time - self.start_time).total_seconds(),
            "status": self.status,
            "total_energy_kwh": round(self.cum_energy_kwh, 9),
            "total_emissions_kg": round(self.cum_emissions_kg, 9),
        }

        try:
            self.kafka_producer.produce(
                topic="training.run_summary",
                value=json.dumps(summary_record).encode("utf-8"),
            )
        except Exception as e:
            print(f"Failed to send run summary to Kafka: {e}")

        print(
            f"Stopped metrics collection. Totals: "
            f"{self.cum_energy_kwh:.6f} kWh, {self.cum_emissions_kg:.6f} kg CO2. "
            f"Status={self.status}"
        )

        # Do not suppress exceptions
        return False