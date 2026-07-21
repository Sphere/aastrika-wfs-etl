"""Writes the Spark summary DataFrame into Postgres, idempotently.

Spark's JDBC writer has no `ON CONFLICT` equivalent, so the pattern here is:
bulk-write via JDBC into a fresh per-run staging table, then run a single
INSERT ... SELECT ... ON CONFLICT (mid) DO NOTHING merge from the driver via
the existing psycopg2 connection pool -- table DDL/merge is inherently a
single-node operation, no need to route it through Spark.
"""

import logging
from uuid import uuid4

from pyspark.sql import DataFrame

from aastrika_telemetry.config.postgres_config import PostgresConfig
from aastrika_telemetry.config.settings import app_config
from aastrika_telemetry.loaders.postgres_loader import PostgresLoader

logger = logging.getLogger(__name__)

_SUMMARY_COLUMNS = (
    "user_id, session_id, content_id, course_id, channel_id, "
    "platform_id, device_id, mid, start_ets, end_ets, "
    "start_imputed, end_imputed, total_time_duration, event_env"
)


def _jdbc_url() -> str:
    return f"jdbc:postgresql://{app_config.postgres_host}:{app_config.postgres_port}/{app_config.postgres_db}"


def _jdbc_properties() -> dict:
    return {
        "user": app_config.postgres_user,
        "password": app_config.postgres_password,
        "driver": "org.postgresql.Driver",
    }


def load_summaries(summary_df: DataFrame) -> int:
    """Bulk-writes `summary_df` to Postgres, deduped by `mid`.

    Returns the number of rows staged (not necessarily the number actually
    inserted -- `ON CONFLICT DO NOTHING` may silently skip some).
    """
    # Reuse the existing DDL bootstrap unchanged.
    PostgresLoader()

    staging_table = f"{app_config.postgres_schema}.telemetry_summary_staging_{uuid4().hex[:8]}"

    # Cache before the first action: both .count() and .write.jdbc() below are
    # separate actions, and without caching, Spark would re-run the whole
    # upstream DAG (parsing + groupBy/applyInPandas) for each one -- silently
    # doubling every accumulator (skipped/valid/non-positive counts) upstream.
    summary_df = summary_df.cache()
    try:
        staged_count = summary_df.count()
        if staged_count == 0:
            logger.info("No summaries to load.")
            return 0

        try:
            summary_df.write.jdbc(
                url=_jdbc_url(),
                table=staging_table,
                mode="overwrite",
                properties=_jdbc_properties(),
            )
        except Exception:
            logger.error(
                "JDBC staging write failed -- skipping merge (nothing was written to %s).",
                app_config.postgres_table,
            )
            raise
    finally:
        summary_df.unpersist()

    merge_sql = f"""
        INSERT INTO {app_config.postgres_table} ({_SUMMARY_COLUMNS})
        SELECT {_SUMMARY_COLUMNS}
        FROM {staging_table}
        ON CONFLICT (mid) DO NOTHING;
    """

    with PostgresConfig.get_cursor() as cur:
        cur.execute(merge_sql)
        cur.execute(f"DROP TABLE IF EXISTS {staging_table};")

    logger.info(
        ">>>>>>>>>>>>>>> Merged %d staged summaries into %s. <<<<<<<<<<<<<<<",
        staged_count,
        app_config.postgres_table,
    )
    return staged_count
