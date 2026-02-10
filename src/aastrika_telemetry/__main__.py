"""
Entry point for running aastrika_telemetry as a module.

Usage:
    python -m aastrika_telemetry
    uv run python -m aastrika_telemetry
"""

import logging
from aastrika_telemetry.config.es_config import ElasticsearchConfig
from aastrika_telemetry.config.postgres_config import PostgresConfig
from aastrika_telemetry.pipeline.orchestrator import TelemetryOrchestrator
from logging.handlers import RotatingFileHandler
import os
from aastrika_telemetry.config.settings import app_config

# Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )

# logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)
# logger = logging.getLogger(__name__)

def setup_logging():
    """Configure logging based on environment settings."""
    handlers = []
    
    # Console handler (always present)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)
    
    # File handler (only if enabled)
    if app_config.log_to_file:
        os.makedirs(app_config.log_dir, exist_ok=True)
        file_handler = RotatingFileHandler(
            f"{app_config.log_dir}/{app_config.log_file_name}",
            maxBytes=app_config.log_max_bytes,
            backupCount=app_config.log_backup_count
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, app_config.log_level.upper()),
        handlers=handlers
    )
    
    # Suppress verbose ES logs
    logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the ETL pipeline."""

    print("\n" + "─" * 60)
    print("  Aastrika Telemetry ETL Service")
    print("  Version: 0.1.0")
    print("  Starting...")
    print("─" * 60 + "\n")


    # Run the ETL pipeline
    orchestrator = TelemetryOrchestrator()
    orchestrator.run_pipeline()


if __name__ == "__main__":
    main()
