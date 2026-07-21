"""
Entry point for running the PySpark ETL pipeline.

Usage:
    uv run python spark_main.py

Mirrors src/aastrika_telemetry/__main__.py's logging + email-notification
wrapper exactly, but drives SparkTelemetryOrchestrator instead of
TelemetryOrchestrator -- the existing `python -m aastrika_telemetry` entry
point is untouched and still works independently.
"""

import logging
import os
from logging.handlers import RotatingFileHandler

from aastrika_telemetry.config.settings import app_config
from aastrika_telemetry.pipeline.spark_orchestrator import SparkTelemetryOrchestrator
from aastrika_telemetry.utils.email_utils import send_email


def setup_logging():
    """Configure logging based on environment settings (same as __main__.py)."""
    handlers = []

    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    if app_config.log_to_file:
        os.makedirs(app_config.log_dir, exist_ok=True)
        file_handler = RotatingFileHandler(
            f"{app_config.log_dir}/{app_config.log_file_name}",
            maxBytes=app_config.log_max_bytes,
            backupCount=app_config.log_backup_count,
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(
        level=getattr(logging, app_config.log_level.upper()), handlers=handlers
    )

    logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)
    logging.getLogger("py4j").setLevel(logging.WARNING)


setup_logging()
logger = logging.getLogger(__name__)


def main():
    print("\n" + "-" * 60, flush=True)
    print("  Aastrika Telemetry ETL Service (PySpark)", flush=True)
    print("  Starting...", flush=True)
    print("-" * 60 + "\n", flush=True)

    try:
        orchestrator = SparkTelemetryOrchestrator()
        result = orchestrator.run()
        send_email("Telemetry WFS Pipeline (Spark) - SUCCESS", result)
    except Exception as e:
        logger.error("Spark pipeline failed: %s", str(e))
        send_email("Telemetry WFS Pipeline (Spark) - FAILED", f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
