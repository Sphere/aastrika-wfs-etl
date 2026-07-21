"""PySpark ETL orchestrator -- reads from Elasticsearch, aggregates durations
via Spark's native groupBy, writes to Postgres via JDBC + a merge.

This is a net-new, additive pipeline path. The existing `TelemetryOrchestrator`
(pipeline/orchestrator.py) is untouched, so the two can be run side by side to
verify parity before anything is cut over.
"""

import logging
import time

from aastrika_telemetry.extractors.es_spark_reader import read_events_df
from aastrika_telemetry.loaders.postgres_spark_writer import load_summaries
from aastrika_telemetry.spark_config import build_spark_session
from aastrika_telemetry.transformers.telemetry_aggregator_spark import aggregate

logger = logging.getLogger(__name__)


class SparkTelemetryOrchestrator:
    """Runs the PySpark ETL pipeline end to end."""

    def run(self) -> dict:
        start_time = time.time()
        spark = build_spark_session()

        try:
            read_result = read_events_df(spark)

            summary_df, non_positive_acc = aggregate(read_result.events_df, spark)

            # The ONE action for the whole pipeline: load_summaries caches
            # summary_df and calls .count() then .write.jdbc() on the cached
            # result. Do not call .count()/.collect()/.show() on events_df or
            # summary_df anywhere else -- that would re-run the parsing and
            # aggregation stages and double-count every accumulator below.
            loaded_to_db = load_summaries(summary_df)

            valid_event_count = read_result.valid_event_count_acc.value
            skipped_events = read_result.skipped_event_count_acc.value
            non_positive_skipped = non_positive_acc.value
            total_events = valid_event_count + skipped_events
            # load_summaries() returns the staged row count, i.e. every group
            # that survived the non-positive-duration filter -- that's exactly
            # "valid summaries" (ON CONFLICT DO NOTHING may skip some at merge
            # time, but that's dedup against prior runs, not invalidity).
            valid_summaries = loaded_to_db

            elapsed = time.time() - start_time
            hours, remainder = divmod(elapsed, 3600)
            minutes, seconds = divmod(remainder, 60)
            duration_str = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

            execution_stats = {
                "status": "SUCCESS",
                "es_index": read_result.es_index,
                "duration": duration_str,
                "total_events": total_events,
                "valid_events": valid_event_count,
                "skipped_events": skipped_events,
                "valid_summaries": valid_summaries,
                "non_positive_skipped": non_positive_skipped,
                "loaded_to_db": loaded_to_db,
            }

            logger.info(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Spark pipeline completed. <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
            )
            logger.info(f"Pipeline Summary:\n{execution_stats}")

            return execution_stats

        finally:
            spark.stop()
