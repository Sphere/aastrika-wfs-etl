"""
Entry point for running aastrika_telemetry as a module.

Usage:
    python -m aastrika_telemetry
    uv run python -m aastrika_telemetry
"""

import logging
import os
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

from aastrika_telemetry.config.es_config import ElasticsearchConfig
from aastrika_telemetry.config.postgres_config import PostgresConfig
from aastrika_telemetry.config.settings import app_config
from aastrika_telemetry.pipeline.orchestrator import TelemetryOrchestrator
from aastrika_telemetry.utils.email_utils import send_email

# Configure logging


def setup_logging():
    """Configure logging based on environment settings."""
    handlers = []

    # Console handler (always present)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    # File handler (only if enabled)
    if app_config.log_to_file:
        os.makedirs(app_config.log_dir, exist_ok=True)
        file_handler = RotatingFileHandler(
            f"{app_config.log_dir}/{app_config.log_file_name}",
            maxBytes=app_config.log_max_bytes,
            backupCount=app_config.log_backup_count,
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, app_config.log_level.upper()), handlers=handlers
    )

    # Suppress verbose ES logs
    logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)


# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)


def automate_backdate():
    """Automate backdate process for historical data."""
    logger.info("Starting backdate process for historical data...")
    # Implement backdate logic here (e.g., loop through past dates, run pipeline for each date)

    start_date = datetime.strptime("04-02-2026", "%d-%m-%Y")
    end_date = datetime.strptime("15-02-2026", "%d-%m-%Y")

    current_date = start_date
    while current_date <= end_date:
        index_name = f"betalaunch-telemetry-{current_date.strftime('%Y-%m-%d')}"
        print(index_name)
        # e.g. betalaunch-telemetry-2026-01-18, betalaunch-telemetry-2026-01-19, ...

        app_config.es_index = index_name
        app_config.fetch_start_date = current_date.strftime("%d-%m-%Y")

        print(
            f"Running pipeline for index: {index_name} and date: {app_config.fetch_start_date}"
        )

        # Run back date ETL pipeline
        try:
            orchestrator = TelemetryOrchestrator()
            result = orchestrator.run_streaming_pipeline()
            send_email("Telemetry WFS Pipeline - SUCCESS", result)
        except Exception as e:
            logger.error("Pipeline failed: %s", str(e))
            send_email("Telemetry WFS Pipeline - FAILED", f"Error: {str(e)}")
            raise

        current_date += timedelta(days=1)


def main():
    """Main entry point for the ETL pipeline."""

    print("\n" + "─" * 60, flush=True)
    print("  Aastrika Telemetry ETL Service", flush=True)
    print("  Version: 0.1.0", flush=True)
    print("  Starting...", flush=True)
    print("─" * 60 + "\n", flush=True)

    automate_backdate()

    # Run the ETL pipeline
    # try:
    #     orchestrator = TelemetryOrchestrator()
    #     result = orchestrator.run_streaming_pipeline()
    #     send_email("Telemetry WFS Pipeline - SUCCESS", result)
    # except Exception as e:
    #     logger.error("Pipeline failed: %s", str(e))
    #     send_email("Telemetry WFS Pipeline - FAILED", f"Error: {str(e)}")
    #     raise


if __name__ == "__main__":
    main()
