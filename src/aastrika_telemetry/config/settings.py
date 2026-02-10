from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    """Global application settings loaded from .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application Settings
    app_name: str
    app_version: str
    debug: bool

    # Elasticsearch Settings
    es_host: str
    es_port: int
    es_scheme: str
    es_username: str | None = None
    es_password: str | None = None
    es_index: str
    es_timeout: int
    es_max_retries: int
    es_index_pattern: str
    ex_index_pattern_set: bool = False

    # PostgreSQL Settings
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_schema: str
    postgres_pool_size: int
    postgres_max_overflow: int
    postgres_table: str

    # ETL Pipeline Settings
    batch_size: int
    max_workers: int
    retry_attempts: int
    retry_delay: int
    hours_window: int
    fetch_start_date: str

    # Logging Settings
    log_to_file: bool = False
    log_dir: str = "logs"
    log_file_name: str = "telemetry_etl.log"
    log_max_bytes: int = 10485760  # 10MB
    log_backup_count: int = 30
    log_level: str = "INFO"

    @property
    def es_url(self) -> str:
        """Generate Elasticsearch URL from components."""
        return f"{self.es_scheme}://{self.es_host}:{self.es_port}"

    @property
    def postgres_dsn(self) -> str:
        """Generate PostgreSQL connection string."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


# Global settings instance
app_config = AppConfig()
