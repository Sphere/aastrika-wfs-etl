"""Elasticsearch extractor - fetches data from Elasticsearch."""

from datetime import datetime, timedelta
import logging
from elasticsearch import Elasticsearch
from aastrika_telemetry.config.es_config import get_es_client
from aastrika_telemetry.config.settings import app_config
from typing import Any
from aastrika_telemetry.models.events import Actor, TelemetryEvent, EventData, EventObject, Context

logger = logging.getLogger(__name__)

class ElasticsearchExtractor:
    """Extract telemetry data from Elasticsearch."""

    def __init__(self):
        self.es_client: Elasticsearch = get_es_client()

        self.es_index_pattern = app_config.es_index_pattern

        if app_config.ex_index_pattern_set:
            self.es_index = self.es_index_pattern.replace("*", datetime.now().strftime("%Y-%m-%d"))
        else:
            self.es_index = app_config.es_index

        logger.info("<<<<<<<<<<< Elastic Search index used: %s >>>>>>>>>>>", self.es_index)




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
            start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(hours=hours_window)

            #TODO: testing purpose only, remove later
            start_time = datetime.now() - timedelta(hours=100)
            end_time = datetime.now()

            start_time_ms = int(start_time.timestamp() * 1000)
            end_time_ms = int(end_time.timestamp() * 1000)

            filters.append(
                {
                    "range": {
                        "telemetry.events.ets": {"gte": start_time_ms, "lte": end_time_ms}
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
    
    

    def extract(self, query: dict | None = None, max_docs: int | None = None) -> list[TelemetryEvent]:
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


            while hits:
                all_hits.extend(hits)

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
            return events


        except Exception as e:
            logger.error(f"Error fetching events: {e}")
            return []




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