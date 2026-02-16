"""ETL Pipeline runner - orchestrates Extract, Transform, Load."""

# TODO: Implement proper resource cleanup for database connections
# - Add __enter__ and __exit__ methods to make ETLPipeline a context manager
# - Call ElasticsearchConfig.close_client() and PostgresConfig.close_pool() in __exit__
# - Update __main__.py to use: with ETLPipeline() as pipeline: pipeline.run()

import json
import logging
import time
from calendar import c
from math import log

from aastrika_telemetry import pipeline
from aastrika_telemetry.config.settings import app_config
from aastrika_telemetry.extractors.es_extractor import ElasticsearchExtractor
from aastrika_telemetry.loaders.postgres_loader import PostgresLoader
from aastrika_telemetry.models import summary
from aastrika_telemetry.models.events import TelemetryEvent
from aastrika_telemetry.models.summary import TelemetrySummary
from aastrika_telemetry.transformers.telemetry_aggregator import TelemetryAggregator
from aastrika_telemetry.utils.memory_monitor import log_memory_usage

logger = logging.getLogger(__name__)


class TelemetryOrchestrator:
    """Main ETL pipeline orchestrator."""

    def __init__(self):
        self.ec_extractor = ElasticsearchExtractor()
        # self.transformer = TelemetryAggregator()
        self.postgres_loader = PostgresLoader()

    def run_streaming_pipeline(self) -> dict[str, int] | str:
        """
        Run pipeline with session-based streaming (memory efficient).

        Returns:
            Summary of pipeline run results
        """
        start_time = time.time()

        # Build session-sorted query
        query = self.ec_extractor.build_session_sorted_query()

        total_loaded = 0
        session_count = 0
        cumulative_valid_summaries = 0
        cumulative_non_positive_count = 0
        cumulative_valid_event_count = 0
        cumulative_skipped_events = 0
        cumulative_total_events = 0

        pending_summaries: list[TelemetrySummary] = []

        # Process sessions one at a time
        for session_events in self.ec_extractor.extract_by_session(query):
            session_count += 1

            # Transform this session
            summaries, stats = TelemetryAggregator.transform(session_events)

            cumulative_valid_summaries += stats["valid_event_summaries"]
            cumulative_non_positive_count += stats["non_positive_summary_count"]
            cumulative_valid_event_count += stats["valid_event_count"]
            cumulative_skipped_events += stats["skipped_events"]
            cumulative_total_events += len(session_events)

            if summaries:
                pending_summaries.extend(summaries)

            # Write to DB when batch is full
            if len(pending_summaries) >= app_config.db_batch_size:
                loaded = self.postgres_loader.load_summaries(pending_summaries)
                total_loaded += loaded
                pending_summaries = []

            # Log progress every 1000 sessions
            if session_count % 1000 == 0:
                log_memory_usage(
                    f"After {session_count} sessions - total loaded summeries in DB: {total_loaded}"
                )

        # Flush remaining summaries
        if pending_summaries:
            loaded = self.postgres_loader.load_summaries(pending_summaries)
            total_loaded += loaded

        log_memory_usage("Pipeline completed")
        logger.info(
            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Streaming pipeline completed. <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
        )

        elapsed = time.time() - start_time
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)

        execution_summary = (
            f"Status: SUCCESS\n"
            f"ES Index: {self.ec_extractor.es_index}\n"
            f"Duration: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}\n"
            f"Sessions Processed: {session_count}\n"
            f"Total Events: {cumulative_total_events}\n"
            f"Valid Events: {cumulative_valid_event_count}\n"
            f"Skipped Events: {cumulative_skipped_events}\n"
            f"Valid Summaries: {cumulative_valid_summaries}\n"
            f"Non-positive Skipped: {cumulative_non_positive_count}\n"
            f"Loaded to DB: {total_loaded}"
        )

        execution_stats = {
            "status": "SUCCESS",
            "es_index": self.ec_extractor.es_index,
            "duration": f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}",
            "sessions_processed": session_count,
            "total_events": cumulative_total_events,
            "valid_events": cumulative_valid_event_count,
            "skipped_events": cumulative_skipped_events,
            "valid_summaries": cumulative_valid_summaries,
            "non_positive_skipped": cumulative_non_positive_count,
            "loaded_to_db": total_loaded,
        }

        logger.info(f"Pipeline Summary:\n{execution_stats}")

        return execution_stats

    # This method is kept for reference but is not recommended for large datasets due to high memory usage.
    def _run_batch_pipeline(self) -> int:
        """
        Run pipeline with batch loading (legacy, high memory usage).

        Returns:
            Number of records loaded
        """
        logger.info("Using BATCH mode (legacy, loads all data in memory)")

        query = self.ec_extractor.build_query(
            app_config.hours_window, app_config.batch_size
        )
        telemetryEventlist: list[TelemetryEvent] = self.ec_extractor.extract(query)
        log_memory_usage("After extraction", len(telemetryEventlist))

        telemetry_summaries, stats = TelemetryAggregator.transform(telemetryEventlist)
        log_memory_usage("After transformation", len(telemetry_summaries))

        loaded_count = self.postgres_loader.load_summaries(telemetry_summaries)
        log_memory_usage("After loading to DB", loaded_count)

        logger.info(
            f">>>>>>>>>>>>>>> Pipeline completed. Loaded {loaded_count} records. <<<<<<<<<<<<<<"
        )
        log_memory_usage("Pipeline completed")

        return loaded_count
