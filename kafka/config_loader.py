import yaml
from pathlib import Path


class Config:
    """Loads and provides access to the YAML configuration file."""

    def __init__(self, path: str = None):
        # Default path relative to this file
        default_path = Path(__file__).parent / "config.yaml"
        self.path = Path(path) if path else default_path

        if not self.path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.path}")

        with open(self.path, "r") as f:
            self.config = yaml.safe_load(f)

    def get(self, *keys, default=None):
        """Access nested config values using dot-like syntax.
        Example: config.get('kafka', 'bootstrap_servers')
        """
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def kafka(self):
        """Shortcut for accessing Kafka configuration."""
        return self.config.get("kafka", {})

    def paths(self):
        """Shortcut for accessing file paths."""
        return self.config.get("paths", {})

    def training(self):
        """Shortcut for accessing training parameters."""
        return self.config.get("training", {})


# Example usage:
if __name__ == "__main__":
    config = Config()
    print("Kafka bootstrap server:", config.get("kafka", "bootstrap_servers"))
    print("Training model type:", config.get("training", "model_type"))