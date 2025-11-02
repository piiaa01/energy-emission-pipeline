import json
from confluent_kafka import Producer


def report(err, msg):
    """Callback function executed after each message is sent."""
    if err:
        print(f"Sending to Kafka failed: {err}")
    else:
        print(f"Sent to Kafka: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")


def send_output_file_to_kafka(
    output_file: str,
    server: str = "localhost:9092",
    topic: str = "training.metrics"
):
    """
    Read JSON lines from an output file and send each record to Kafka.
    Each line in the file must contain a valid JSON object.
    """
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
                producer.poll(0)  # process delivery callbacks
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {e}")

    producer.flush()


class KafkaProducerWrapper:
    """Simple wrapper around the Kafka Producer for easier usage."""

    def __init__(self, server: str = "localhost:9092"):
        self.kafka_producer = Producer({"bootstrap.servers": server})

    def produce(self, topic: str, value: bytes):
        """
        Send a single message to Kafka.
        Call 'flush()' after producing all messages to ensure delivery.
        """
        self.kafka_producer.produce(
            topic=topic,
            value=value,
            callback=report
        )
        self.kafka_producer.poll(0)

    def flush(self):
        """Block until all messages in the producer queue are delivered."""
        self.kafka_producer.flush()