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
from aastrika_telemetry.utils.memory_monitor import log_memory_usage
import logging
import json

logger = logging.getLogger(__name__)

class TelemetryOrchestrator:
    """Main ETL pipeline orchestrator."""

    def __init__(self):
        self.ec_extractor = ElasticsearchExtractor()
        # self.transformer = TelemetryAggregator()
        self.postgres_loader = PostgresLoader()

    def run_pipeline(self, query: dict | None = None, use_streaming: bool = True) -> int:
        """
        Run the complete ETL pipeline.

        Args:
            query: Optional Elasticsearch query
            use_streaming: Use session-based streaming (default: True, recommended for large datasets)

        Returns:
            Number of records loaded
        """
        logger.info("Starting ETL Pipeline...")
        log_memory_usage("Pipeline started")

        if use_streaming:
            return self._run_streaming_pipeline()
        else:
            return self._run_batch_pipeline()

    def _run_streaming_pipeline(self) -> int:
        """
        Run pipeline with session-based streaming (memory efficient).

        Returns:
            Number of records loaded
        """
        logger.info("Using STREAMING mode (session-based, memory efficient)")

        # Build session-sorted query
        query = self.ec_extractor.build_session_sorted_query()

        total_loaded = 0
        session_count = 0
        cumulative_loop_count = 0
        cumulative_negative_count = 0
        cumulative_summaries_count = 0

        # Process sessions one at a time
        for session_events in self.ec_extractor.extract_by_session(query):
            session_count += 1

            # Transform this session
            summaries, stats = TelemetryAggregator.transform(session_events)

            cumulative_loop_count += stats['loop_count']
            cumulative_negative_count += stats['negative_duration_count']
            cumulative_summaries_count += stats['summaries_count']

            # Load this session's summaries
            if summaries:
                loaded = self.postgres_loader.load_summaries(summaries)
                total_loaded += loaded

            # Log progress every 100 sessions
            if session_count % 100 == 0:
                logger.info(
                    f"Processed {session_count} sessions, loaded {total_loaded} records | "
                    f"Cumulative: records={cumulative_loop_count}, "
                    f"negative_skipped={cumulative_negative_count}, "
                    f"summaries={cumulative_summaries_count}"
                )
                log_memory_usage(f"After {session_count} sessions", total_loaded)

        logger.info(
            f">>>>>>>>>>>>>>> Streaming pipeline completed. "
            f"Processed {session_count} sessions, loaded {total_loaded} records. <<<<<<<<<<<<<<"
        )

        logger.info(
        f"############### FINAL STATS: Total records={cumulative_loop_count}, "
        f"Negative skipped={cumulative_negative_count}, "
        f"Valid summaries={cumulative_summaries_count} ###############"
)
        log_memory_usage("Pipeline completed")

        return total_loaded

    def _run_batch_pipeline(self) -> int:
        """
        Run pipeline with batch loading (legacy, high memory usage).

        Returns:
            Number of records loaded
        """
        logger.info("Using BATCH mode (legacy, loads all data in memory)")

        query = self.ec_extractor.build_query(app_config.hours_window, app_config.batch_size)
        telemetryEventlist: list[TelemetryEvent] = self.ec_extractor.extract(query)
        log_memory_usage("After extraction", len(telemetryEventlist))

        telemetry_summaries, stats = TelemetryAggregator.transform(telemetryEventlist)
        log_memory_usage("After transformation", len(telemetry_summaries))

        loaded_count = self.postgres_loader.load_summaries(telemetry_summaries)
        log_memory_usage("After loading to DB", loaded_count)

        logger.info(f">>>>>>>>>>>>>>> Pipeline completed. Loaded {loaded_count} records. <<<<<<<<<<<<<<")
        log_memory_usage("Pipeline completed")

        return loaded_count

        
