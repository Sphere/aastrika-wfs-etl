"""Reads telemetry events from Elasticsearch directly into a Spark DataFrame.

Uses the elasticsearch-hadoop connector's Hadoop InputFormat API (raw JSON per
document) rather than the `org.elasticsearch.spark.sql` DataFrame source,
because the SQL source infers its schema from the ES index mapping -- and this
index's documents disagree on nesting depth (`telemetry.events.*` vs
`telemetry.*` vs flat), which the mapping can't represent cleanly. Reading raw
JSON and parsing it ourselves (mirroring `ElasticsearchExtractor.parse_es_event`)
sidesteps that entirely.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from aastrika_telemetry.config.settings import app_config

logger = logging.getLogger(__name__)

EVENT_SCHEMA = StructType(
    [
        StructField("eid", StringType(), True),
        StructField("ets", LongType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("platform_id", StringType(), True),
        StructField("content_id", StringType(), True),
        StructField("course_id", StringType(), True),
    ]
)


@dataclass
class ReadResult:
    events_df: DataFrame
    es_index: str
    event_start_time: datetime
    event_end_time: datetime
    # Only reliable AFTER an action has run on `events_df` (or anything derived
    # from it) -- Spark accumulators only reflect completed task execution.
    # Trigger exactly one action over the whole pipeline (the final JDBC write)
    # before reading `.value`; triggering a second action (e.g. an extra
    # `.count()` for debugging) will double-count, since the mapPartitions
    # parsing stage would run again.
    skipped_event_count_acc: "object"
    valid_event_count_acc: "object"


def _resolve_index_and_window() -> tuple[str, datetime, datetime]:
    """Port of `ElasticsearchExtractor.__init__`'s index/date resolution logic."""
    if app_config.es_index_pattern_set is False:
        if app_config.fetch_start_date == "":
            raise ValueError(
                "FETCH_START_DATE must have date(DD-MM-YYYY) value if ES_INDEX_PATTERN_SET=False"
            )
        event_start_time = datetime.strptime(app_config.fetch_start_date, "%d-%m-%Y")
        es_index = app_config.es_index
    else:
        event_start_time = (datetime.now() - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        es_index = app_config.es_index_pattern.replace(
            "*", event_start_time.strftime("%Y-%m-%d")
        )

    event_end_time = event_start_time + timedelta(hours=app_config.hours_window)
    return es_index, event_start_time, event_end_time


def _build_filter_query(event_start_time: datetime, event_end_time: datetime) -> dict:
    """Same eid/ets filters as the legacy query -- no `sort` clause, since
    Spark's own groupBy shuffle doesn't need pre-sorted input."""
    start_ms = int(event_start_time.timestamp() * 1000)
    end_ms = int(event_end_time.timestamp() * 1000)
    return {
        "query": {
            "bool": {
                "filter": [
                    {
                        "terms": {
                            "telemetry.events.eid.keyword": [
                                "START",
                                "END",
                                "INTERACT",
                                "IMPRESSION",
                            ]
                        }
                    },
                    {
                        "range": {
                            "telemetry.events.ets": {"gte": start_ms, "lte": end_ms}
                        }
                    },
                ]
            }
        }
    }


def _make_parse_partition(skipped_acc, valid_acc):
    """Builds the mapPartitions function -- a near-literal port of
    `ElasticsearchExtractor.parse_es_event`'s defensive shape-unwrapping,
    plus the required-field filter from `TelemetryAggregator.transform`."""

    def parse_partition(json_lines):
        for line in json_lines:
            try:
                doc = json.loads(line)
            except Exception:
                continue

            if "telemetry" in doc:
                source = doc["telemetry"]
                event_data = source.get("events", source) if isinstance(source, dict) else source
            else:
                event_data = doc

            context_data = event_data.get("context") or {}
            actor_data = event_data.get("actor") or {}
            object_data = event_data.get("object") or {}

            session_id = context_data.get("sid")
            user_id = actor_data.get("id")
            channel = context_data.get("channel")
            pdata = context_data.get("pdata") or {}
            platform_id = pdata.get("id") if isinstance(pdata, dict) else None

            content_id = object_data.get("id") if object_data else None
            course_id = None
            rollup = object_data.get("rollup") if object_data else None
            if isinstance(rollup, dict):
                course_id = rollup.get("l1")

            # Matches telemetry_aggregator.py's required-field guard exactly.
            if not session_id or not user_id or not content_id or not course_id:
                skipped_acc.add(1)
                continue

            valid_acc.add(1)
            yield (
                event_data.get("eid", ""),
                int(event_data.get("ets") or 0),
                user_id,
                session_id,
                channel,
                platform_id,
                content_id,
                course_id,
            )

    return parse_partition


def read_events_df(spark: SparkSession) -> ReadResult:
    """Reads matching telemetry events from Elasticsearch into a DataFrame."""
    es_index, event_start_time, event_end_time = _resolve_index_and_window()
    query = _build_filter_query(event_start_time, event_end_time)

    logger.info(">>>>>>>>>>> Elasticsearch index used: %s <<<<<<<<<<<", es_index)
    logger.info(
        ">>>>>>>>>>> Fetch window: %s - %s <<<<<<<<<<<", event_start_time, event_end_time
    )

    es_read_conf = {
        "es.nodes": app_config.es_host,
        "es.port": str(app_config.es_port),
        "es.resource": es_index,
        "es.query": json.dumps(query),
        "es.output.json": "true",
        "es.nodes.wan.only": "true",
    }
    if app_config.es_username and app_config.es_password:
        es_read_conf["es.net.http.auth.user"] = app_config.es_username
        es_read_conf["es.net.http.auth.pass"] = app_config.es_password

    sc = spark.sparkContext
    raw_rdd = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.apache.hadoop.io.Text",
        conf=es_read_conf,
    )
    json_rdd = raw_rdd.values()

    skipped_acc = sc.accumulator(0)
    valid_acc = sc.accumulator(0)

    events_rdd = json_rdd.mapPartitions(_make_parse_partition(skipped_acc, valid_acc))
    events_df = spark.createDataFrame(events_rdd, schema=EVENT_SCHEMA)
    events_df = events_df.withColumn("_ingest_seq", F.monotonically_increasing_id())

    return ReadResult(
        events_df=events_df,
        es_index=es_index,
        event_start_time=event_start_time,
        event_end_time=event_end_time,
        skipped_event_count_acc=skipped_acc,
        valid_event_count_acc=valid_acc,
    )
