"""Distributed port of TelemetryAggregator.transform's per-group duration calc.

The per-group logic itself (find first START, find last qualifying END else
fall back to the last event, compute duration, discard non-positive/no-START
groups, build `mid`) is a near-literal copy of
`transformers/telemetry_aggregator.py` -- only the grouping mechanism changed,
from a Python dict keyed by composite key to Spark's own `groupBy` shuffle.
"""

import logging

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

SUMMARY_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("content_id", StringType(), True),
        StructField("course_id", StringType(), True),
        StructField("channel_id", StringType(), True),
        StructField("platform_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("mid", StringType(), True),
        StructField("start_ets", LongType(), True),
        StructField("end_ets", LongType(), True),
        StructField("start_imputed", BooleanType(), True),
        StructField("end_imputed", BooleanType(), True),
        StructField("total_time_duration", LongType(), True),
        StructField("event_env", StringType(), True),
    ]
)

_EMPTY = pd.DataFrame(columns=[f.name for f in SUMMARY_SCHEMA.fields])


def _make_aggregate_group(non_positive_acc):
    """Builds the per-group function passed to `groupBy(...).applyInPandas(...)`.

    Every branch must return a DataFrame with exactly SUMMARY_SCHEMA's columns
    (never `None`) -- applyInPandas requires that.
    """

    def aggregate_group(pdf: pd.DataFrame) -> pd.DataFrame:
        if pdf.empty:
            return _EMPTY

        # Stable sort -- matches the tie-break semantics of Python's list.sort()
        # in the legacy code, using `_ingest_seq` (assigned once, pre-shuffle) as
        # the surrogate for "original list order".
        pdf = pdf.sort_values(["ets", "_ingest_seq"], kind="mergesort")

        starts = pdf[pdf["eid"] == "START"]
        if starts.empty:
            # No START in this group -- discard entirely, same as the legacy code.
            return _EMPTY
        start_row = starts.iloc[0]
        start_ets = int(start_row["ets"])

        ends_after_start = pdf[(pdf["eid"] == "END") & (pdf["ets"] >= start_ets)]
        if not ends_after_start.empty:
            end_ets = int(ends_after_start.iloc[-1]["ets"])
            end_imputed = False
        else:
            end_ets = int(pdf.iloc[-1]["ets"])
            end_imputed = True

        # Milliseconds despite the legacy name `duration_sec` -- preserved as-is.
        duration = end_ets - start_ets
        if duration <= 0:
            non_positive_acc.add(1)
            return _EMPTY

        k = pdf.iloc[0]
        mid = f"SESCNT_{start_ets}_{k['session_id']}_{k['content_id']}_{k['course_id']}"

        return pd.DataFrame(
            [
                {
                    "user_id": k["user_id"],
                    "session_id": k["session_id"],
                    "content_id": k["content_id"],
                    "course_id": k["course_id"],
                    "channel_id": start_row["channel"],
                    "platform_id": start_row["platform_id"],
                    "device_id": None,
                    "mid": mid,
                    "start_ets": start_ets,
                    "end_ets": end_ets,
                    "start_imputed": False,  # We always have START (required)
                    "end_imputed": end_imputed,
                    "total_time_duration": duration,
                    "event_env": None,
                }
            ]
        )

    return aggregate_group


def aggregate(events_df: DataFrame, spark: SparkSession):
    """Groups events by (session, content, course, user) and computes durations.

    Returns (summary_df, non_positive_summary_count_acc). The accumulator is
    only reliable AFTER an action has run over summary_df (or anything derived
    from it) -- see the same caveat in es_spark_reader.ReadResult.
    """
    non_positive_acc = spark.sparkContext.accumulator(0)

    summary_df = events_df.groupBy(
        "session_id", "content_id", "course_id", "user_id"
    ).applyInPandas(_make_aggregate_group(non_positive_acc), schema=SUMMARY_SCHEMA)

    return summary_df, non_positive_acc
