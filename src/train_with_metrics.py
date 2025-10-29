from collector.collector_context import MetricsCollector
import kafka.producer
import time

def train_dummy(epochs=3, steps=100):
    """Simulates a simple training loop."""
    for e in range(epochs):
        print(f"Epoch {e+1}/{epochs}")
        for s in range(steps):
            # Simulated training computation
            _ = (e + 1) * (s + 1) ** 0.5
            if s % 20 == 0:
                print(f"  Step {s}/{steps}")
            time.sleep(0.05)  # Simulated processing delay

if __name__ == "__main__":
    output_file="metrics_run002.txt"
    
    # Start the metrics collector during training
    with MetricsCollector(
        output_file=output_file,
        run_id="run_002",
        user_id="tnm",
        model_name="dummy_model",
        region="DE",
        interval_s=2
    ):
        train_dummy()

    print(f"Training completed. Metrics saved to {output_file}.")
    
    kafka.producer.send_output_to_kafka(output_file)