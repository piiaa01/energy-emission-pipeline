"""
Kafka producer for external data sources (e.g. AWS, Azure, external logs).

Current project:
    - Training metrics are already sent to Kafka by the MetricsCollector
      inside train_with_metrics.py.
    - Therefore, this producer is not used yet.

Future use:
    - Integrate cloud metrics (AWS, Azure, GCP, on-prem logs, etc.)
    - Read data from external APIs, object storage or monitoring services
    - Normalize records and send them to the same Kafka topics used
      by the training pipeline, e.g.:
        - training.metrics
        - training.run_summary
"""

import json
import sys
from typing import Dict, Any

from confluent_kafka import Producer
from kafka.config_loader import Config


class ExternalKafkaProducer:
    """
    Generic Kafka producer for external data sources.

    Intended usage (future):
        - Instantiate once per service / job.
        - Pull data from an external source (AWS, Azure, APIs, logs, etc.).
        - Normalize records to the internal schema.
        - Send them to Kafka (metrics or summary topics).

    This class deliberately does NOT implement any concrete data
    fetching logic yet. It only centralizes:
        - Kafka connection setup
        - JSON serialization
        - error handling
    """

    def __init__(self) -> None:
        config = Config()
        self.metrics_topic = config.get("kafka", "topic_training_metrics")
        self.summary_topic = "training.run_summary"
        self.server = config.get("kafka", "bootstrap_servers")

        producer_conf = {
            "bootstrap.servers": self.server,
        }
        self.producer = Producer(producer_conf)

        print(
            f"[ExternalKafkaProducer] Initialized. Kafka server={self.server}, "
            f"metrics_topic={self.metrics_topic}, summary_topic={self.summary_topic}"
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _delivery_report(self, err, msg) -> None:
        """Callback for Kafka delivery results."""
        if err is not None:
            print(f"[ExternalKafkaProducer] ERROR delivering message: {err}", file=sys.stderr)
        else:
            print(
                f"[ExternalKafkaProducer] Delivered message to "
                f"{msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

    def _send(self, topic: str, record: Dict[str, Any]) -> None:
        """
        Serialize record as JSON and send it to the given topic.

        This method should be used by higher-level helpers such as:
            - send_metrics_record(...)
            - send_summary_record(...)
        """
        try:
            payload = json.dumps(record).encode("utf-8")
        except (TypeError, ValueError) as e:
            print(
                f"[ExternalKafkaProducer] Failed to serialize record to JSON: {e}; "
                f"record={record!r}",
                file=sys.stderr,
            )
            return

        try:
            self.producer.produce(
                topic=topic,
                value=payload,
                callback=self._delivery_report,
            )
            # Trigger delivery callbacks
            self.producer.poll(0)
        except BufferError as e:
            print(f"[ExternalKafkaProducer] Local producer queue is full: {e}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Public API: to be used by future integrations
    # ------------------------------------------------------------------
    def send_metrics_record(self, record: Dict[str, Any]) -> None:
        """
        Send a single metrics record to the training.metrics topic.

        The record should already be normalized to the internal schema, e.g.:
            {
                "timestamp": "...",
                "project_id": "...",
                "user_id": "...",
                "run_id": "...",
                "energy_kwh": ...,
                "emissions_kg": ...,
                ...
            }

        This method does not modify the record; it only sends it.
        """
        self._send(self.metrics_topic, record)

    def send_summary_record(self, record: Dict[str, Any]) -> None:
        """
        Send a single run summary record to the training.run_summary topic.

        Again, the record is expected to be normalized beforehand.
        """
        self._send(self.summary_topic, record)

    def flush(self) -> None:
        """Flush all buffered messages before shutting down."""
        print("[ExternalKafkaProducer] Flushing producer...")
        self.producer.flush()


# Note:
# There is intentionally no __main__ entrypoint here.
# This module is meant to be imported and used by future
# integration code (e.g. aws_ingest.py, azure_ingest.py, etc.),
# not executed as a standalone script at this stage.
