import time
import math
import random

from collector.collector_context import MetricsCollector
from kafka.config_loader import Config


def train_with_metrics():
    """
    Training loop that reports training state (epoch, step, loss, accuracy)
    to the MetricsCollector.

    Currently this is a simulated training loop, but you can easily replace
    the inner part with a real model training loop.
    """
    config = Config()
    training_cfg = config.training()
    project_env = config.get("project", "environment", default="local")

    model_type = training_cfg.get("model_type", "dummy_model")
    learning_rate = float(training_cfg.get("learning_rate", 0.01))
    epochs = int(training_cfg.get("epochs", 3))
    batch_size = int(training_cfg.get("batch_size", 32))
    dataset_name = training_cfg.get("dataset_name", "dummy_dataset")

    output_file = config.get("paths", "output_file", default="./data/output.jsonl")

    
    user_id = "pia"
    region = "DE"
    framework = "sklearn"

    run_id = f"run_{int(time.time())}"

    print(
        f"Starting training run_id={run_id}, model={model_type}, "
        f"epochs={epochs}, batch_size={batch_size}, lr={learning_rate}"
    )

    with MetricsCollector(
        output_file=output_file,
        run_id=run_id,
        user_id=user_id,
        model_name=model_type,
        region=region,
        interval_s=2.0,
        dataset_name=dataset_name,
        framework=framework,
        hyperparameters=training_cfg,
        environment=project_env,
    ) as collector:
        global_step = 0
        steps_per_epoch = 100  # for the simulated example

        for epoch in range(1, epochs + 1):
            print(f"Epoch {epoch}/{epochs}")

            for step in range(1, steps_per_epoch + 1):
                global_step += 1

                # Simulated "training": loss decays, accuracy increases over time.
                progress = global_step / float(epochs * steps_per_epoch)
                loss = max(
                    0.1,
                    2.0 * math.exp(-3 * progress) + random.uniform(-0.05, 0.05),
                )
                accuracy = min(
                    0.99,
                    0.5 + 0.5 * progress + random.uniform(-0.02, 0.02),
                )

                # Update collector with current training state
                collector.update_training_state(
                    epoch=epoch,
                    step=step,
                    loss=loss,
                    accuracy=accuracy,
                )

                # Simulated compute time per step
                time.sleep(0.05)

                if step % 20 == 0:
                    print(
                        f"  Step {step}/{steps_per_epoch} "
                        f"- loss={loss:.4f}, acc={accuracy:.4f}"
                    )

    print("Training completed. Metrics and run summary sent to Kafka.")
    print(f"Output file (local JSONL) path: {output_file}")


if __name__ == "__main__":
    train_with_metrics()