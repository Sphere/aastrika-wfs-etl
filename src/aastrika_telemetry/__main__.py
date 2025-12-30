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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_connections():
    """Test connections to Elasticsearch and PostgreSQL."""
    print("=" * 60)
    print("Testing Database Connections...")
    print("=" * 60)

    # Test Elasticsearch
    print("\n1. Testing Elasticsearch connection...")
    if ElasticsearchConfig.test_connection():
        print("   ✓ Elasticsearch connection successful!")
    else:
        print("   ✗ Elasticsearch connection failed!")
        return False

    # Test PostgreSQL
    print("\n2. Testing PostgreSQL connection...")
    if PostgresConfig.test_connection():
        print("   ✓ PostgreSQL connection successful!")
    else:
        print("   ✗ PostgreSQL connection failed!")
        return False

    print("\n" + "=" * 60)
    print("All connections successful!")
    print("=" * 60)
    return True


def main():
    """Main entry point for the ETL pipeline."""
    print("\n🚀 Aastrika Telemetry ETL Service\n")

    # Test connections first
    # if not test_connections():
    #     print("\n Connection tests failed. Please check your .env configuration.")
    #     return

    # Run the ETL pipeline
    orchestrator = TelemetryOrchestrator()
    loaded_count = orchestrator.run_pipeline()

    logger.info(f"ETL Pipeline finished. Total records loaded: {loaded_count}")



if __name__ == "__main__":
    main()
