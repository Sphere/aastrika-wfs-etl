"""ETL Pipeline runner - orchestrates Extract, Transform, Load."""

# TODO: Implement proper resource cleanup for database connections
# - Add __enter__ and __exit__ methods to make ETLPipeline a context manager
# - Call ElasticsearchConfig.close_client() and PostgresConfig.close_pool() in __exit__
# - Update __main__.py to use: with ETLPipeline() as pipeline: pipeline.run()

from aastrika_telemetry.extractors.es_extractor import ElasticsearchExtractor
from aastrika_telemetry.loaders.postgres_loader import PostgresLoader
from aastrika_telemetry.config.settings import app_config
from aastrika_telemetry.models.events import TelemetryEvent
from aastrika_telemetry.transformers.telemetry_aggregator import TelemetryAggregator
from aastrika_telemetry.models.summary import TelemetrySummary
import logging
import json

logger = logging.getLogger(__name__)

class TelemetryOrchestrator:
    """Main ETL pipeline orchestrator."""

    def __init__(self):
        self.ec_extractor = ElasticsearchExtractor()
        # self.transformer = TelemetryAggregator()
        self.postgres_loader = PostgresLoader()

    def run_pipeline(self, query: dict | None = None) -> int:
        """
        Run the complete ETL pipeline.

        Args:
            query: Optional Elasticsearch query

        Returns:
            Pipeline execution summary
        """
        logger.info("Starting ETL Pipeline...")

        query = self.ec_extractor.build_query(app_config.hours_window, app_config.batch_size)
        telemetryEventlist: list[TelemetryEvent] = self.ec_extractor.extract(query)

        telemetry_summaries: list[TelemetrySummary] = TelemetryAggregator.transform(telemetryEventlist)

        loaded_count = self.postgres_loader.load_summaries(telemetry_summaries)

        logger.info(f">>>>>>>>>>>>>>> Pipeline completed. Loaded {loaded_count} records. <<<<<<<<<<<<<<")

        return loaded_count

        
