import json
import signal
import sys
from typing import Optional, Dict, Any
import os

from confluent_kafka import Consumer
from kafka.config_loader import Config
from pymongo import MongoClient, errors as pymongo_errors


class KafkaTracker:
    """
    Consumes Kafka messages from:
    - training.metrics      (time-series metrics)
    - training.run_summary  (per-run summary)
    and writes them into MongoDB:
    - energy_emissions.training_metrics
    - energy_emissions.run_summaries
    """

    def __init__(self):
        config = Config()
        self.metrics_topic = config.get("kafka", "topic_training_metrics")
        self.summary_topic = "training.run_summary"
        self.server = config.get("kafka", "bootstrap_servers")
        self.group_id = config.get("kafka", "group_id")
        self.offset = config.get("kafka", "auto_offset_reset", default="earliest")

        # Kafka consumer
        consumer_conf = {
            "bootstrap.servers": self.server,
            "group.id": self.group_id,
            "auto.offset.reset": self.offset,
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([self.metrics_topic, self.summary_topic])

        
        mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")

        try:
            self.mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.mongo_client.server_info()  # test connection
            self.db = self.mongo_client["energy_emissions"]
            self.metrics_collection = self.db["training_metrics"]
            self.summary_collection = self.db["run_summaries"]
            print(
                f"[KafkaTracker] Connected to MongoDB at {mongo_uri}, "
                f"db=energy_emissions (collections: training_metrics, run_summaries)"
            )
        except Exception as e:
            print(f"[KafkaTracker] ERROR connecting to MongoDB: {e}", file=sys.stderr)
            self.mongo_client = None
            self.db = None
            self.metrics_collection = None
            self.summary_collection = None

        print(
            f"[KafkaTracker] Initialized. Kafka server={self.server}, "
            f"metrics_topic={self.metrics_topic}, summary_topic={self.summary_topic}, "
            f"group_id={self.group_id}, offset={self.offset}"
        )

    # -----------------------------
    # Mongo helpers
    # -----------------------------
    def _store_metrics(self, record: Dict[str, Any]) -> Optional[str]:
        if self.metrics_collection is None:
            print("[KafkaTracker] metrics collection not available – skipping insert.", file=sys.stderr)
            return None
        try:
            result = self.metrics_collection.insert_one(record)
            print(f"[KafkaTracker] Stored metrics record with _id={result.inserted_id}")
            return str(result.inserted_id)
        except pymongo_errors.PyMongoError as e:
            print(f"[KafkaTracker] ERROR inserting metrics into MongoDB: {e}", file=sys.stderr)
            return None

    def _store_summary(self, record: Dict[str, Any]) -> Optional[str]:
        if self.summary_collection is None:
            print("[KafkaTracker] run_summaries collection not available – skipping insert.", file=sys.stderr)
            return None
        try:
            result = self.summary_collection.insert_one(record)
            print(f"[KafkaTracker] Stored run summary with _id={result.inserted_id}")
            return str(result.inserted_id)
        except pymongo_errors.PyMongoError as e:
            print(f"[KafkaTracker] ERROR inserting run summary into MongoDB: {e}", file=sys.stderr)
            return None

    # -----------------------------
    # Normalization helpers
    # -----------------------------
    @staticmethod
    def _normalize_metrics_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure that the dashboard-friendly fields are always present:
        - energy_kwh
        - emissions_kg
        They are derived from interval or cumulative fields if needed.
        """
        data = dict(payload)

        # Energy
        if "energy_kwh" not in data:
            if "energy_kwh_interval" in data:
                data["energy_kwh"] = data["energy_kwh_interval"]
            elif "cumulative_energy_kwh" in data:
                data["energy_kwh"] = data["cumulative_energy_kwh"]

        # Emissions
        if "emissions_kg" not in data:
            if "emissions_kg_interval" in data:
                data["emissions_kg"] = data["emissions_kg_interval"]
            elif "cumulative_emissions_kg" in data:
                data["emissions_kg"] = data["cumulative_emissions_kg"]

        return data

    # -----------------------------
    # Main consume loop
    # -----------------------------
    def track(self) -> None:
        """
        Main loop:
        - poll Kafka
        - decode UTF-8
        - parse JSON
        - route by topic to the appropriate collection
        """
        print(
            f"[KafkaTracker] Starting to consume from topics: "
            f"{self.metrics_topic}, {self.summary_topic}"
        )
        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(f"[KafkaTracker] Kafka error: {msg.error()}", file=sys.stderr)
                    continue

                topic = msg.topic()
                raw_value = msg.value()
                try:
                    text = raw_value.decode("utf-8")
                except Exception as e:
                    print(f"[KafkaTracker] Failed to decode UTF-8: {e}; raw={raw_value!r}", file=sys.stderr)
                    continue

                try:
                    payload = json.loads(text)
                except json.JSONDecodeError as e:
                    print(f"[KafkaTracker] JSON decode error: {e}; text={text!r}", file=sys.stderr)
                    continue

                if not isinstance(payload, dict):
                    print(
                        f"[KafkaTracker] Unexpected payload type from topic {topic}: "
                        f"{type(payload)}; payload={payload!r}",
                        file=sys.stderr,
                    )
                    continue

                if topic == self.metrics_topic:
                    normalized = self._normalize_metrics_payload(payload)
                    self._store_metrics(normalized)
                elif topic == self.summary_topic:
                    self._store_summary(payload)
                else:
                    # Should not happen, but keep for debugging.
                    print(f"[KafkaTracker] Received message from unexpected topic {topic}", file=sys.stderr)

        except KeyboardInterrupt:
            print("[KafkaTracker] Stopping tracker (KeyboardInterrupt)...")
        finally:
            print("[KafkaTracker] Closing Kafka consumer and Mongo client...")
            try:
                self.consumer.close()
            except Exception:
                pass
            if self.mongo_client is not None:
                try:
                    self.mongo_client.close()
                except Exception:
                    pass


def _run() -> None:
    tracker = KafkaTracker()

    def _stop(*_):
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    tracker.track()


if __name__ == "__main__":
    _run()