import json
import uuid
from confluent_kafka import Producer

def report(err, msg):
    if err:
        print(f"❌ Sending to Kafka failed: {err}")
    else:
        # print(f"✅ Delivered {msg.value().decode('utf-8')}")
        print(f"✅ Sent to topic {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")

def send_output_to_kafka(output_file:str):
    """Read records from output file and send to Kafka."""
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    
    with open(output_file, 'r') as file:
        output_file_data = [json.loads(line) for line in file]
        
    for record in output_file_data:
        value = json.dumps(record).encode("utf-8")
        producer.produce(
            topic="orders",
            value=value,
            callback=report
        )
    producer.flush()
    
with open("metrics_run002.txt", 'r') as file:
    output_file_data = [json.loads(line) for line in file]
print(output_file_data[0])