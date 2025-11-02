import json
from confluent_kafka import Producer
from kafka.config_loader import Config


def report(err, msg):
    """Callback executed after each message is sent."""
    if err:
        print(f"Sending to Kafka failed: {err}")
    else:
        print(f"Sent to Kafka: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")


def send_output_file_to_kafka(output_file: str):
    """
    Read JSON lines from a file and send each record to the configured Kafka topic.
    """
    config = Config()
    server = config.get("kafka", "bootstrap_servers")
    topic = config.get("kafka", "topic_training_metrics")

    producer = Producer({"bootstrap.servers": server})

    with open(output_file, "r") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                value = json.dumps(record).encode("utf-8")
                producer.produce(topic=topic, value=value, callback=report)
                producer.poll(0)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {e}")

    producer.flush()


class KafkaProducerWrapper:
    """Simple wrapper for producing messages to Kafka."""

    def __init__(self):
        config = Config()
        self.server = config.get("kafka", "bootstrap_servers")
        self.topic = config.get("kafka", "topic_training_metrics")
        self.producer = Producer({"bootstrap.servers": self.server})

    def produce(self, value: bytes, topic: str = None):
        """Send a single message to Kafka."""
        topic = topic or self.topic
        self.producer.produce(topic=topic, value=value, callback=report)
        self.producer.poll(0)

    def flush(self):
        """Ensure all queued messages are delivered."""
        self.producer.flush()


if __name__ == "__main__":
    producer = KafkaProducerWrapper()
    message = json.dumps({"user_id": "markus", "acc": 0.91}).encode("utf-8")
    producer.produce(message)
    producer.flush()