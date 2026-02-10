"""
Aastrika Telemetry ETL Service
Entry point for the ETL pipeline: Elasticsearch -> Transform -> PostgreSQL
"""

import logging

from aastrika_telemetry.config.es_config import ElasticsearchConfig
from aastrika_telemetry.config.postgres_config import PostgresConfig


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

    # Test connections first
    if not test_connections():
        print("\n Connection tests failed. Please check your .env configuration.")
        return


if __name__ == "__main__":
    main()
