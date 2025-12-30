import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from aastrika_telemetry.config.settings import app_config


class PostgresConfig:
    """PostgreSQL connection configuration and pool management."""

    _connection_pool: pool.SimpleConnectionPool | None = None

    @classmethod
    def get_pool(cls) -> pool.SimpleConnectionPool:
        """
        Get or create PostgreSQL connection pool (singleton pattern).

        Returns:
            SimpleConnectionPool: PostgreSQL connection pool
        """
        if cls._connection_pool is None:
            cls._connection_pool = cls._create_pool()
        return cls._connection_pool

    @classmethod
    def _create_pool(cls) -> pool.SimpleConnectionPool:
        """
        Create new PostgreSQL connection pool with configuration from settings.

        Returns:
            SimpleConnectionPool: Configured connection pool
        """
        return pool.SimpleConnectionPool(
            minconn=1,
            maxconn=app_config.postgres_pool_size + app_config.postgres_max_overflow,
            host=app_config.postgres_host,
            port=app_config.postgres_port,
            database=app_config.postgres_db,
            user=app_config.postgres_user,
            password=app_config.postgres_password,
            options=f"-c search_path={app_config.postgres_schema}",
        )

    @classmethod
    def close_pool(cls) -> None:
        """Close all connections in the pool."""
        # TODO: This method is currently not called. Should be invoked when ETL pipeline completes.
        # Consider implementing context manager pattern in ETLPipeline to ensure cleanup.
        if cls._connection_pool is not None:
            cls._connection_pool.closeall()
            cls._connection_pool = None

    @classmethod
    @contextmanager
    def get_connection(cls):
        """
        Context manager to get a connection from the pool.

        Usage:
            with PostgresConfig.get_connection() as conn:
                # use connection
                pass

        Yields:
            connection: PostgreSQL connection from pool
        """
        conn = None
        try:
            conn = cls.get_pool().getconn()
            yield conn
        finally:
            if conn is not None:
                cls.get_pool().putconn(conn)

    @classmethod
    @contextmanager
    def get_cursor(cls, cursor_factory=RealDictCursor):
        """
        Context manager to get a cursor from a pooled connection.

        Usage:
            with PostgresConfig.get_cursor() as cursor:
                cursor.execute("SELECT * FROM table")
                results = cursor.fetchall()

        Args:
            cursor_factory: Cursor factory class (default: RealDictCursor for dict results)

        Yields:
            cursor: PostgreSQL cursor
        """
        with cls.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

    @classmethod
    def test_connection(cls) -> bool:
        """
        Test PostgreSQL connection.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            with cls.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            print(f"PostgreSQL connection failed: {e}")
            return False


# Convenience functions
def get_postgres_connection():
    """Get PostgreSQL connection from pool."""
    return PostgresConfig.get_connection()


def get_postgres_cursor(cursor_factory=RealDictCursor):
    """Get PostgreSQL cursor with automatic connection management."""
    return PostgresConfig.get_cursor(cursor_factory=cursor_factory)
