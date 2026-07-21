"""PostgreSQL loader - loads transformed data into PostgreSQL."""

from psycopg2.extras import execute_batch
from aastrika_telemetry.config.postgres_config import PostgresConfig
from aastrika_telemetry.config.settings import app_config
from aastrika_telemetry.models.summary import TelemetrySummary
import logging

logger = logging.getLogger(__name__)

class PostgresLoader:
    """Load transformed data into PostgreSQL."""
    def __init__(self):
        self.create_table_if_not_exists()


    def load_summaries(self, summaries: list[TelemetrySummary]) -> int:
        if not summaries:
            return 0

        insert_query = f"""
            INSERT INTO {app_config.postgres_table} (
                user_id, session_id, content_id, course_id, channel_id,
                platform_id, device_id, mid,
                start_ets, end_ets,
                start_imputed, end_imputed,
                total_time_duration,
                event_env
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s,
                      %s, %s,
                      %s,
                      %s)
            ON CONFLICT (mid) DO NOTHING
        """

        # Prepare batch data
        data_batch = [
            (
                summary.user_id,
                summary.session_id,
                summary.content_id,
                summary.course_id,
                summary.channel_id,
                summary.platform_id,
                summary.device_id,
                summary.mid,
                summary.start_ets,
                summary.end_ets,
                summary.start_imputed,
                summary.end_imputed,
                summary.total_time_duration,
                summary.event_env
            )
            for summary in summaries
        ]

        with PostgresConfig.get_cursor() as cursor:
            execute_batch(cursor, insert_query, data_batch, page_size=app_config.batch_size)

        return len(summaries)


    def create_table_if_not_exists(self) -> None:
        if not self.is_table_exists(app_config.postgres_table):
            logger.info(f">>>>>>>>>>>>>>>>>> Creating table {app_config.postgres_table} as it does not exist. <<<<<<<<<<<<<<<<<<")

            with PostgresConfig.get_cursor() as cursor:
                cursor.execute(self.get_create_table_query())

            create_indexes_query = f"""
            CREATE INDEX IF NOT EXISTS idx_content_id ON {app_config.postgres_table}(content_id);
            CREATE INDEX IF NOT EXISTS idx_user_id ON {app_config.postgres_table}(user_id);
            CREATE INDEX IF NOT EXISTS idx_session_id ON {app_config.postgres_table}(session_id);
            CREATE INDEX IF NOT EXISTS idx_course_id ON {app_config.postgres_table}(course_id);
            """
            with PostgresConfig.get_cursor() as cursor:
                cursor.execute(create_indexes_query)

    

    def is_table_exists(self, table_name: str) -> bool:
        with PostgresConfig.get_cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
                """,
                (table_name,)
            )

            return cursor.fetchone()["exists"]
        

    def get_create_table_query(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {app_config.postgres_table} (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(255),
            session_id VARCHAR(255),
            content_id VARCHAR(255),
            course_id VARCHAR(255),
            channel_id VARCHAR(255),
            platform_id VARCHAR(255),
            device_id VARCHAR(255),
            mid VARCHAR(255) UNIQUE,
            start_ets BIGINT,
            end_ets BIGINT,
            start_imputed BOOLEAN DEFAULT FALSE,
            end_imputed BOOLEAN DEFAULT FALSE,
            total_time_duration DOUBLE PRECISION,
            event_env VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    