import json
import time
from datetime import datetime
from typing import Dict, Any, Tuple

import psutil
from confluent_kafka import Producer
from kafka.config_loader import Config

EMISSION_FACTOR_KG_PER_KWH = 0.4  # simple fixed factor


class MetricsCollector:
    """
    Collects energy/emission metrics during training and sends them to Kafka
    AND writes them to a local JSONL file.

    - Periodically: record -> Kafka topic `training.metrics`
    - On exit: run summary -> Kafka topic `training.run_summary`
    """

    def __init__(
        self,
        output_file: str,
        run_id: str,
        user_id: str,
        model_name: str,
        region: str,
        interval_s: float,
        dataset_name: str,
        framework: str,
        hyperparameters: Dict[str, Any],
        environment: str = "local",
    ):
        self.output_file = output_file
        self.run_id = run_id
        self.user_id = user_id
        self.model_name = model_name
        self.region = region
        self.interval_s = float(interval_s)
        self.dataset_name = dataset_name
        self.framework = framework
        self.hyperparameters = dict(hyperparameters)
        self.environment = environment

        self._file = None

        # Cumulative metrics
        self.cum_energy_kwh = 0.0
        self.cum_emissions_kg = 0.0
        self._last_ts = None
        self._last_send_ts = 0.0
        self._total_steps = 0

        cfg = Config()
        self.metrics_topic = cfg.get("kafka", "topic_training_metrics")
        self.summary_topic = cfg.get(
            "kafka",
            "topic_training_run_summary",
            default="training.run_summary",
        )
        self.bootstrap_servers = cfg.get("kafka", "bootstrap_servers")

        self.producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        print(
            f"[MetricsCollector] Kafka producer to {self.bootstrap_servers} "
            f"(metrics_topic={self.metrics_topic}, summary_topic={self.summary_topic})"
        )

    def _delivery_report(self, err, msg) -> None:
        """Kafka delivery callback for debugging message delivery."""
        if err is not None:
            print(f"[MetricsCollector] ❌ Kafka delivery failed: {err}")
        else:
            print(
                f"[MetricsCollector] ✅ Kafka delivered "
                f"topic={msg.topic()} partition={msg.partition()}"
            )

    def __enter__(self) -> "MetricsCollector":
        self._file = open(self.output_file, "a", encoding="utf-8")
        self._last_ts = time.time()
        print(f"[MetricsCollector] Started. Writing local JSONL to {self.output_file}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        status = "success" if exc_type is None else "failed"

        summary = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "run_id": self.run_id,
            "user_id": self.user_id,
            "model_name": self.model_name,
            "dataset_name": self.dataset_name,
            "region_iso": self.region,
            "framework": self.framework,
            "environment": self.environment,
            "total_energy_kwh": self.cum_energy_kwh,
            "total_emissions_kg": self.cum_emissions_kg,
            "total_steps": self._total_steps,
            "status": status,
        }

        self._write_local(summary)
        self._send_kafka(summary, topic=self.summary_topic)

        try:
            self.producer.flush()
        except Exception as e:
            print(f"[MetricsCollector] Error flushing Kafka: {e}")

        if self._file is not None:
            self._file.close()

        print(
            f"[MetricsCollector] Stopped. total_energy={self.cum_energy_kwh:.6f} kWh, "
            f"emissions={self.cum_emissions_kg:.6f} kg, status={status}"
        )
        return False

    def update_training_state(self, epoch: int, step: int, loss: float, accuracy: float) -> None:
        now = time.time()
        dt = 0.0 if self._last_ts is None else now - self._last_ts
        self._last_ts = now

        interval_energy_kwh, interval_emissions_kg, cpu_util_pct = self._estimate_interval(dt)

        self.cum_energy_kwh += interval_energy_kwh
        self.cum_emissions_kg += interval_emissions_kg
        self._total_steps += 1

        record = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "run_id": self.run_id,
            "user_id": self.user_id,
            "model_name": self.model_name,
            "dataset_name": self.dataset_name,
            "region_iso": self.region,
            "framework": self.framework,
            "environment": self.environment,
            "epoch": epoch,
            "step": step,
            "loss": float(loss),
            "accuracy": float(accuracy),
            "cpu_utilization_pct": float(cpu_util_pct),
            "energy_kwh": interval_energy_kwh,
            "emissions_kg": interval_emissions_kg,
            "cumulative_energy_kwh": self.cum_energy_kwh,
            "cumulative_emissions_kg": self.cum_emissions_kg,
        }

        self._write_local(record)

        if now - self._last_send_ts >= self.interval_s:
            self._send_kafka(record, topic=self.metrics_topic)
            self._last_send_ts = now

    def _estimate_interval(self, dt_s: float) -> Tuple[float, float, float]:
        """Estimate energy and emissions for the last time interval."""
        if dt_s <= 0:
            return 0.0, 0.0, 0.0

        cpu_util_pct = psutil.cpu_percent(interval=None)  # 0..100
        cpu_util = cpu_util_pct / 100.0

        # Very rough proxy: CPU TDP * utilization
        tdp_w = 65.0
        power_w = tdp_w * cpu_util

        energy_kwh = power_w * dt_s / 3600.0 / 1000.0
        emissions_kg = energy_kwh * EMISSION_FACTOR_KG_PER_KWH
        return energy_kwh, emissions_kg, cpu_util_pct

    def _write_local(self, record: Dict[str, Any]) -> None:
        """Write a JSONL record to the local output file."""
        if self._file is None:
            return
        self._file.write(json.dumps(record) + "\n")
        self._file.flush()

    def _send_kafka(self, record: Dict[str, Any], topic: str) -> None:
        """Send a JSON record to Kafka."""
        try:
            payload = json.dumps(record).encode("utf-8")
            self.producer.produce(
                topic=topic,
                value=payload,
                on_delivery=self._delivery_report,
            )
            self.producer.poll(0)
        except Exception as e:
            print(f"[MetricsCollector] Failed to send to Kafka topic={topic}: {e}")
