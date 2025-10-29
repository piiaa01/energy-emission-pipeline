from collector.collector_context import MetricsCollector
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
    # Start the metrics collector during training
    with MetricsCollector(
        output_file="metrics_run001.json",
        run_id="run_001",
        user_id="pia",
        model_name="dummy_model",
        region="DE",
        interval_s=2
    ):
        train_dummy()

    print("Training completed. Metrics saved to metrics_run001.json.")