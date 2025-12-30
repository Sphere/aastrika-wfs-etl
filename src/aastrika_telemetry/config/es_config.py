from elasticsearch import Elasticsearch
from aastrika_telemetry.config.settings import app_config


class ElasticsearchConfig:
    """Elasticsearch connection configuration and client management."""

    _client: Elasticsearch | None = None

    @classmethod
    def get_client(cls) -> Elasticsearch:
        """
        Get or create Elasticsearch client instance (singleton pattern).

        Returns:
            Elasticsearch: Configured Elasticsearch client
        """
        if cls._client is None:
            cls._client = cls._create_client()
        return cls._client

    @classmethod
    def _create_client(cls) -> Elasticsearch:
        """
        Create new Elasticsearch client with configuration from settings.

        Returns:
            Elasticsearch: Configured Elasticsearch client
        """
        es_config = {
            "hosts": [app_config.es_url],
            "request_timeout": app_config.es_timeout,
            "max_retries": app_config.es_max_retries,
            "retry_on_timeout": True,
        }

        # Add authentication if credentials are provided
        if app_config.es_username and app_config.es_password:
            es_config["basic_auth"] = (app_config.es_username, app_config.es_password)

        return Elasticsearch(**es_config)

    @classmethod
    def close_client(cls) -> None:
        """Close the Elasticsearch client connection."""
        # TODO: This method is currently not called. Should be invoked when ETL pipeline completes.
        # Consider implementing context manager pattern in ETLPipeline to ensure cleanup.
        if cls._client is not None:
            cls._client.close()
            cls._client = None

    @classmethod
    def test_connection(cls) -> bool:
        """
        Test Elasticsearch connection.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            client = cls.get_client()
            return client.ping()
        except Exception as e:
            print(f"Elasticsearch connection failed: {e}")
            return False


# Convenience function to get ES client
def get_es_client() -> Elasticsearch:
    """Get Elasticsearch client instance."""
    return ElasticsearchConfig.get_client()
