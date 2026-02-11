"""Elasticsearch extractor - fetches data from Elasticsearch."""

import logging
from datetime import datetime, timedelta
from typing import Any, Generator

from elasticsearch import Elasticsearch

from aastrika_telemetry.config.es_config import get_es_client
from aastrika_telemetry.config.settings import app_config
from aastrika_telemetry.models.events import (
    Actor,
    Context,
    EventData,
    EventObject,
    TelemetryEvent,
)
from aastrika_telemetry.utils.memory_monitor import log_memory_usage

logger = logging.getLogger(__name__)


class ElasticsearchExtractor:
    """Extract telemetry data from Elasticsearch."""

    def __init__(self):
        self.es_client: Elasticsearch = get_es_client()

        if app_config.fetch_start_date:
            self.event_start_time = datetime.strptime(
                app_config.fetch_start_date, "%d-%m-%Y"
            )
        else:
            self.event_start_time = (datetime.now() - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        self.event_end_time = self.event_start_time + timedelta(
            hours=app_config.hours_window
        )

        self.es_index_pattern = app_config.es_index_pattern

        if app_config.ex_index_pattern_set:
            self.es_index = self.es_index_pattern.replace(
                "*", self.event_start_time.strftime("%Y-%m-%d")
            )
        else:
            self.es_index = app_config.es_index

        logger.info("_" * 100)

        logger.info(
            ">>>>>>>>>>>>>>>>>>>>> Elastic Search index used: %s <<<<<<<<<<<<<<<<<<<<",
            self.es_index,
        )

        logger.info(
            ">>>>>>>>>>>>>>>>>>>>> Elastic Search Fetch Event Start And End Time: %s - %s <<<<<<<<<<<<<<<<<<<<",
            self.event_start_time,
            self.event_end_time,
        )

    def build_query(self, hours_window: int, batch_size: int) -> dict:
        filters = [
            {
                "terms": {
                    "telemetry.events.eid.keyword": [
                        "START",
                        "END",
                        "INTERACT",
                        "IMPRESSION",
                    ]
                }
            }
        ]

        if hours_window < 1000:
            start_time = datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            end_time = start_time + timedelta(hours=hours_window)

            # #TODO: testing purpose only, remove later #Dangerous to run in production
            # start_time = datetime.now() - timedelta(hours=100)
            # end_time = datetime.now()

            start_time_ms = int(start_time.timestamp() * 1000)
            end_time_ms = int(end_time.timestamp() * 1000)

            filters.append(
                {
                    "range": {
                        "telemetry.events.ets": {
                            "gte": start_time_ms,
                            "lte": end_time_ms,
                        }
                    }
                }  # type: ignore
            )

            logger.info(f"Fetching events from {start_time} to {end_time}")

        else:
            logger.info("Fetching events without time filter")

        query = {
            "query": {"bool": {"filter": filters}},
            "sort": [{"telemetry.events.ets": "asc"}],
            "size": batch_size,
        }

        return query

    def build_session_sorted_query(self) -> dict:
        filters = [
            {
                "terms": {
                    "telemetry.events.eid.keyword": [
                        "START",
                        "END",
                        "INTERACT",
                        "IMPRESSION",
                    ]
                }
            }
        ]

        start_time_ms = int(self.event_start_time.timestamp() * 1000)
        end_time_ms = int(self.event_end_time.timestamp() * 1000)

        filters.append(
            {
                "range": {
                    "telemetry.events.ets": {
                        "gte": start_time_ms,
                        "lte": end_time_ms,
                    }
                }
            }  # type: ignore
        )

        # Sort by session ID only to group all events from same session together
        query = {
            "query": {"bool": {"filter": filters}},
            "sort": [
                {
                    "telemetry.events.context.sid.keyword": {
                        "order": "asc",
                        "missing": "_last",
                    }
                },
                {
                    "telemetry.events.ets": "asc"
                },  # Secondary sort by timestamp within session
            ],
            "size": app_config.batch_size,
        }

        return query

    def extract(
        self, query: dict | None = None, max_docs: int | None = None
    ) -> list[TelemetryEvent]:
        """
        Extract data from Elasticsearch.

        Args:
            query: Elasticsearch query DSL (default: match_all)

        Returns:
            List of documents from Elasticsearch
        """
        # if query is None:
        #     query = {"match_all": {}}

        # # TODO: Implement pagination, batch processing
        # response = self.client.search(
        #     index=self.index,
        #     query=query,
        #     size=app_config.batch_size
        # )

        # return [hit["_source"] for hit in response["hits"]["hits"]]

        try:
            # Use scroll for large result sets
            response = self.es_client.search(
                index=self.es_index, body=query, scroll="2m"
            )

            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]
            all_hits = []

            log_memory_usage("ES Extraction started")

            while hits:
                all_hits.extend(hits)

                # Log memory every 50K records
                if len(all_hits) % 50000 == 0:
                    log_memory_usage("ES Extraction in progress", len(all_hits))

                # Check if we've reached max_docs limit
                if max_docs and len(all_hits) >= max_docs:
                    logger.info(f"Reached max_docs limit: {max_docs}")
                    all_hits = all_hits[:max_docs]  # Trim to exact limit
                    break

                response = self.es_client.scroll(scroll_id=scroll_id, scroll="2m")
                scroll_id = response["_scroll_id"]
                hits = response["hits"]["hits"]

            # Clear scroll
            try:
                self.es_client.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass  # Ignore if scroll already expired TODO: Improve error handling

            logger.info(f"Fetched {len(all_hits)} raw events from Elasticsearch")
            log_memory_usage("ES Extraction completed (raw hits)", len(all_hits))

            # Parse ES documents to TelemetryEvent objects
            events: list[TelemetryEvent] = []

            for hit in all_hits:
                try:
                    event = self.parse_es_event(hit["_source"])
                    events.append(event)
                except Exception as e:
                    logger.warning(f"Failed to parse event: {e}")
                    continue

            logger.info(f"Parsed {len(events)} telemetry events")
            log_memory_usage("ES Extraction completed (parsed events)", len(events))
            return events

        except Exception as e:
            logger.error(f"Error fetching events: {e}")
            return []

    def extract_by_session(
        self, query: dict | None = None
    ) -> Generator[list[TelemetryEvent], None, None]:
        """
        Stream extraction: yields batches of events grouped by session.

        This generator yields complete sessions one at a time, ensuring:
        - All events for a session are together
        - Memory only holds ONE session at a time
        - No split sessions across batches

        Args:
            query: Elasticsearch query (should be session-sorted)

        Yields:
            List of TelemetryEvent objects for one session
        """
        try:
            log_memory_usage("Streaming extraction started")

            # Initiate scroll
            response = self.es_client.search(
                index=self.es_index, body=query, scroll="5m"
            )

            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]

            # State for tracking current session (by session ID only)
            current_session_id = None
            current_session_events: list[TelemetryEvent] = []
            total_events_processed = 0
            total_sessions_yielded = 0

            while hits:
                for hit in hits:
                    try:
                        # Parse event
                        event = self.parse_es_event(hit["_source"])
                        total_events_processed += 1

                        # Extract session ID
                        if not event.context or not event.context.sid:
                            # logger.warning("Event missing session ID, skipping")
                            continue

                        session_id = event.context.sid

                        # Check if we've moved to a new session
                        if (
                            current_session_id is not None
                            and session_id != current_session_id
                        ):
                            # Yield the complete previous session (all events for that session ID)
                            if current_session_events:
                                total_sessions_yielded += 1
                                yield current_session_events
                                current_session_events = []

                        # Update current session
                        current_session_id = session_id
                        current_session_events.append(event)

                    except Exception as e:
                        logger.warning(f"Failed to parse event: {e}")
                        continue

                # Fetch next batch from ES
                response = self.es_client.scroll(scroll_id=scroll_id, scroll="5m")
                scroll_id = response["_scroll_id"]
                hits = response["hits"]["hits"]

            # Yield the last session
            if current_session_events:
                total_sessions_yielded += 1
                log_memory_usage(
                    f"Streamed {total_sessions_yielded} sessions - total unfiltered event collected: {total_events_processed}"
                )
                yield current_session_events

            # Clear scroll
            try:
                self.es_client.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass

            logger.info(
                f">>>>>>>>>>>> Streaming extraction completed - Events Processed: {total_events_processed} <<<<<<<<<<<<<<<"
            )

        except Exception as e:
            logger.error(f"Error in streaming extraction: {e}")
            return

    def parse_es_event(self, es_doc: dict[str, Any]) -> TelemetryEvent:
        """
        Parse Elasticsearch document to TelemetryEvent

        Args:
            es_doc: Elasticsearch document from _source

        Returns:
            TelemetryEvent object
        """
        # Handle nested telemetry structure
        if "telemetry" in es_doc:
            source = es_doc["telemetry"]
            # Extract from events if present
            if "events" in source:
                event_data = source["events"]
            else:
                event_data = source
        else:
            event_data = es_doc

        # Extract context
        context_data = event_data.get("context", {})
        context = Context(
            channel=context_data.get("channel"),
            pdata=context_data.get("pdata"),
            env=context_data.get("env"),
            sid=context_data.get("sid"),
            did=context_data.get("did"),
            cdata=context_data.get("cdata"),
        )

        # Extract edata
        edata_dict = event_data.get("edata", {})
        edata = EventData(
            type=edata_dict.get("type"),
            pageid=edata_dict.get("pageid"),
            mode=edata_dict.get("mode"),
            subtype=edata_dict.get("subtype"),
            id=edata_dict.get("id"),
        )

        # Extract object
        object_dict = event_data.get("object", {})
        event_object = None
        if object_dict:
            event_object = EventObject(
                id=object_dict.get("id"),
                type=object_dict.get("type"),
                ver=object_dict.get("ver"),
                rollup=object_dict.get("rollup"),
            )

        # Extract user/actor
        actor_dict = event_data.get("actor", {})
        actor = None
        if actor_dict:
            actor = Actor(
                id=actor_dict.get("id"),
                type=actor_dict.get("type"),
            )

        # Create TelemetryEvent
        return TelemetryEvent(
            eid=event_data.get("eid", ""),
            ets=event_data.get("ets", 0),
            ver=event_data.get("ver", "3.0"),
            mid=event_data.get("mid"),
            actor=actor,
            context=context,
            object=event_object,
            edata=edata,
        )
