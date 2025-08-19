"""Global configuration constants for the Analitiq Stream framework."""

from pathlib import Path

# Find the project root (where analitiq_stream package is located)
PROJECT_ROOT = Path(__file__).parent.parent

# Centralized directory paths for all pipelines and streams
DIRECTORIES = {
    "state": PROJECT_ROOT / "state",
    "deadletter": PROJECT_ROOT / "deadletter", 
    "logs": PROJECT_ROOT / "logs"
}

# Default configuration
DEFAULT_MONITORING_INTERVAL_SECONDS = 5