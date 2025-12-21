import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import psutil
from confluent_kafka import Producer


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


class MetricsCollector:
    """
    Collects training metrics and sends them to Kafka, while also writing JSONL locally.

    Single-topic design:
      - All events go to KAFKA_TOPIC (default: training.events)
      - Each message includes event_type:
          - "metric" for periodic metrics
          - "run_summary" once at the end

    The class is compatible with:
      with MetricsCollector(...) as collector:
          collector.update_training_state(...)
    """

    def __init__(
        self,
        *,
        output_file: str,
        run_id: str,
        user_id: str,
        model_name: str,
        region: str,
        interval_s: float,
        dataset_name: str,
        framework: str,
        hyperparameters: Optional[Dict[str, Any]] = None,
        environment: str = "local",
    ) -> None:
        self.output_file = output_file
        self.run_id = run_id
        self.user_id = user_id
        self.model_name = model_name
        self.dataset_name = dataset_name
        self.region_iso = region
        self.framework = framework
        self.environment = environment
        self.hyperparameters = hyperparameters or {}

        self.interval_s = float(interval_s)

        self.kafka_bootstrap = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka-svc:9092")
        
        self.kafka_topic = _env("KAFKA_TOPIC", _env("KAFKA_METRICS_TOPIC", "training.events"))

        self._producer = Producer({"bootstrap.servers": self.kafka_bootstrap})

        self._fh = None

        # run lifecycle
        self._start_time: Optional[float] = None
        self._start_timestamp: Optional[str] = None

        # throttling of metric sends
        self._last_emit_time: float = 0.0

        # cumulative values
        self._n_metric_events: int = 0
        self._cumulative_energy_kwh: float = 0.0
        self._cumulative_emissions_kg: float = 0.0

        # simple defaults (if you don't have real energy)
        self._emission_factor_kg_per_kwh = float(_env("EMISSION_FACTOR_KG_PER_KWH", "0.4"))
        self._estimated_avg_power_w = float(_env("ESTIMATED_AVG_POWER_W", "50"))

        # last known training state
        self._epoch: Optional[int] = None
        self._step: Optional[int] = None
        self._loss: Optional[float] = None
        self._accuracy: Optional[float] = None

    # ---------------------------
    # Context manager
    # ---------------------------
    def __enter__(self) -> "MetricsCollector":
        os.makedirs(os.path.dirname(self.output_file) or ".", exist_ok=True)
        self._fh = open(self.output_file, "a", encoding="utf-8")

        self._start_time = time.time()
        self._start_timestamp = _utc_now_iso()
        self._last_emit_time = 0.0

        print(
            f"[collector] started run_id={self.run_id} topic={self.kafka_topic} "
            f"bootstrap={self.kafka_bootstrap} output_file={self.output_file}"
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        status = "completed" if exc_type is None else "failed"
        try:
            # send final summary
            self._emit_run_summary(status=status)
        finally:
            try:
                self._producer.flush(5)
            except Exception:
                pass
            if self._fh:
                try:
                    self._fh.close()
                except Exception:
                    pass

        # Do not suppress exceptions
        return False

    # ---------------------------
    # Public API used by training
    # ---------------------------
    def update_training_state(self, *, epoch: int, step: int, loss: float, accuracy: float) -> None:
        """
        Update current training state and emit a metric event at most once per interval_s.
        """
        self._epoch = int(epoch)
        self._step = int(step)
        self._loss = float(loss)
        self._accuracy = float(accuracy)

        now = time.time()
        if self._start_time is None:
            # Shouldn't happen because we use context manager, but keep safe.
            self._start_time = now
            self._start_timestamp = _utc_now_iso()

        # Throttle sending
        if self._last_emit_time == 0.0 or (now - self._last_emit_time) >= self.interval_s:
            self._emit_metric(now)

    # ---------------------------
    # Internals: building events
    # ---------------------------
    def _base(self) -> Dict[str, Any]:
        return {
            "timestamp": _utc_now_iso(),
            "run_id": self.run_id,
            "user_id": self.user_id,
            "model_name": self.model_name,
            "dataset_name": self.dataset_name,
            "region_iso": self.region_iso,
            "framework": self.framework,
            "environment": self.environment,
            # include hyperparameters as object (Mongo can store dict)
            "hyperparameters": self.hyperparameters,
        }

    def _write_local(self, rec: Dict[str, Any]) -> None:
        if not self._fh:
            return
        self._fh.write(json.dumps(rec) + "\n")
        self._fh.flush()

    def _send_kafka(self, rec: Dict[str, Any]) -> None:
        payload = json.dumps(rec).encode("utf-8")
        self._producer.produce(self.kafka_topic, value=payload, on_delivery=self._delivery_report)
        self._producer.poll(0)

    def _delivery_report(self, err, msg) -> None:
        if err is not None:
            print(f"[collector] delivery failed: {err}")

    def _estimate_energy_emissions(self, duration_s: float) -> Dict[str, Any]:
        energy_kwh = (self._estimated_avg_power_w * float(duration_s)) / 3600.0 / 1000.0
        emissions_kg = float(energy_kwh) * self._emission_factor_kg_per_kwh
        return {
            "energy_kwh": float(energy_kwh),
            "emissions_kg": float(emissions_kg),
        }

    def _emit_metric(self, now_ts: float) -> None:
        # interval since last emit (used for estimating energy)
        duration_s = self.interval_s if self._last_emit_time == 0.0 else max(0.0, now_ts - self._last_emit_time)
        self._last_emit_time = now_ts

        rec = self._base()
        rec["event_type"] = "metric"

        # training state
        if self._epoch is not None:
            rec["epoch"] = self._epoch
        if self._step is not None:
            rec["step"] = self._step
        if self._loss is not None:
            rec["loss"] = self._loss
        if self._accuracy is not None:
            rec["accuracy"] = self._accuracy

        # system metrics
        rec["cpu_utilization_pct"] = float(psutil.cpu_percent(interval=None))

        # energy/emissions (estimate; replace here if you have real measurement)
        ee = self._estimate_energy_emissions(duration_s=duration_s)
        rec.update(ee)

        # cumulative
        self._cumulative_energy_kwh += float(rec["energy_kwh"])
        self._cumulative_emissions_kg += float(rec["emissions_kg"])
        self._n_metric_events += 1

        rec["cumulative_energy_kwh"] = float(self._cumulative_energy_kwh)
        rec["cumulative_emissions_kg"] = float(self._cumulative_emissions_kg)

        # store + send
        self._write_local(rec)
        self._send_kafka(rec)

    def _emit_run_summary(self, status: str) -> None:
        if self._start_time is None:
            # If for some reason never started, still send minimal summary
            self._start_time = time.time()
            self._start_timestamp = _utc_now_iso()

        end_timestamp = _utc_now_iso()
        total_duration_s = float(time.time() - self._start_time)

        rec = self._base()
        rec["event_type"] = "run_summary"
        rec["status"] = status
        rec["start_timestamp"] = self._start_timestamp
        rec["end_timestamp"] = end_timestamp
        rec["total_duration_s"] = total_duration_s
        rec["n_metric_events"] = int(self._n_metric_events)
        rec["total_energy_kwh"] = float(self._cumulative_energy_kwh)
        rec["total_emissions_kg"] = float(self._cumulative_emissions_kg)

        self._write_local(rec)
        self._send_kafka(rec)
