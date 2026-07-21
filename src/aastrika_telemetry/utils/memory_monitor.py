"""Memory monitoring utility for tracking ETL pipeline memory usage."""

import psutil
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def get_memory_info() -> dict[str, float]:
    """
    Get current memory usage information.

    Returns:
        Dictionary with memory metrics in MB and percentage
    """
    process = psutil.Process()
    memory_info = process.memory_info()
    virtual_memory = psutil.virtual_memory()

    return {
        "process_mb": memory_info.rss / 1024 / 1024,  # Process memory in MB
        "available_mb": virtual_memory.available / 1024 / 1024,
        "total_mb": virtual_memory.total / 1024 / 1024,
        "percent": virtual_memory.percent,
    }


def log_memory_usage(stage: str, record_count: Optional[int] = None) -> None:
    """
    Log current memory usage with context.

    Args:
        stage: Description of current pipeline stage
        record_count: Optional count of records processed
    """
    mem = get_memory_info()

    record_info = f" - {record_count:,} records" if record_count else ""

    logger.info(
        f"[MEMORY] {stage}{record_info} | "
        f"Process: {mem['process_mb']:.1f}MB | "
        f"System: {mem['total_mb'] - mem['available_mb']:.1f}MB / {mem['total_mb']:.1f}MB "
        f"({mem['percent']:.1f}%)"
    )

    # Warn if memory usage is high
    if mem['percent'] > 80:
        logger.warning(
            f"⚠️  High memory usage detected: {mem['percent']:.1f}% - "
            f"Consider reducing batch size or implementing streaming"
        )
