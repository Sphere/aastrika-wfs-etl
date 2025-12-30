
from attr import dataclass
from typing import Any


@dataclass
class TelemetryEvent:
     
    eid: str  # Event ID/Type
    ets: int  # Event timestamp (milliseconds)
    ver: str = "3.0"
    mid: str | None = None  # Message ID

    actor: "Actor | None" = None
    context: "Context | None" = None
    object: "EventObject | None" = None
    edata: "EventData | None" = None


@dataclass
class Actor:
    id: str | None = None
    type: str | None = None

@dataclass
class Context:
    channel: str | None = None
    pdata: dict[str, str] | None = None
    env: str | None = None
    sid: str | None = None
    did: str | None = None
    cdata: list[dict[str, Any]] | None = None


@dataclass
class EventObject:
    id: str | None = None
    type: str | None = None
    ver: str | None = None
    rollup: dict[str, str] | None = None


@dataclass
class EventData:
    id: str | None = None
    type: str | None = None
    pageid: str | None = None
    subtype: str | None = None
    mode: str | None = None