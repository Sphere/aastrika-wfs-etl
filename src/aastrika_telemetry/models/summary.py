
from datetime import date
from attr import dataclass


@dataclass
class TelemetrySummary:
    """Data class representing a summary of telemetry data."""
    
    user_id: str | None = None
    session_id: str | None = None
    content_id: str | None = None
    course_id: str | None = None
    channel_id: str | None = None
    platform_id: str | None = None
    device_id: str | None = None
    mid: str | None = None
    summary_date: date | None = None

    start_ets: int | None = None  # Start timestamp in milliseconds
    end_ets: int | None = None  # End timestamp in milliseconds

    # Flags
    start_imputed: bool = False  # Whether start timestamp was imputed
    end_imputed: bool = False  # Whether end timestamp was imputed

    total_time_duration: int | None = None # in milliseconds

    # total_interactions: int | None = None # TODO: Uncomment when interaction data is available
    event_env: str | None = None  # "Home", "Learn", "Profile", etc.

    platform_id: str | None = None