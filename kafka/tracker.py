import json
import signal
import sys
from confluent_kafka import Consumer
from kafka.config_loader import Config


class KafkaTracker:
    """Consumes and prints Kafka messages from the configured topic."""

    def __init__(self):
        config = Config()
        self.topic = config.get("kafka", "topic_training_metrics")
        self.server = config.get("kafka", "bootstrap_servers")
        self.group_id = config.get("kafka", "group_id")
        self.offset = config.get("kafka", "auto_offset_reset", default="earliest")

        self.consumer = Consumer({
            "bootstrap.servers": self.server,
            "group.id": self.group_id,
            "auto.offset.reset": self.offset
        })

        self.consumer.subscribe([self.topic])
        print(f"KafkaTracker initialized and subscribed to topic '{self.topic}'")

    def track(self):
        """Continuously consume messages and print their content."""
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Error from consumer: {msg.error()}", file=sys.stderr)
                    continue

                raw = msg.value().decode("utf-8")
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    print(f"Received non-JSON message: {raw}")
                    continue

                user = record.get("user_id", "<unknown>")
                print(f"Received record from {user}")
        except KeyboardInterrupt:
            print("Stopping KafkaTracker (KeyboardInterrupt)")
        finally:
            self.consumer.close()


def _run():
    tracker = KafkaTracker()

    def _stop(*_):
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    tracker.track()


if __name__ == "__main__":
    _run()