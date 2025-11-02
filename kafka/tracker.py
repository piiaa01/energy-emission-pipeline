import json
import signal
import sys
from confluent_kafka import Consumer


class KafkaTracker:
    def __init__(
        self,
        subscribe_topic: str = "training.metrics",
        server: str = "localhost:9092",
        group_id: str = "record-tracker"
    ):
        self.topic = subscribe_topic
        self.consumer = Consumer({
            "bootstrap.servers": server,
            "group.id": group_id,
            "auto.offset.reset": "earliest"
        })
        self.consumer.subscribe([subscribe_topic])
        print(f"KafkaTracker initialized and subscribed to topic '{subscribe_topic}'")

    def track(self):
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
    tracker = KafkaTracker(
        subscribe_topic="training.metrics",
        server="localhost:9092",
        group_id="record-tracker"
    )

    # Graceful shutdown on SIGTERM/SIGINT
    def _stop(*_):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    tracker.track()


if __name__ == "__main__":
    _run()