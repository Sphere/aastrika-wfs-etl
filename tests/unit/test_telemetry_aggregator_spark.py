"""Unit tests for the per-group duration calculation used by the PySpark
aggregator. Exercises `aggregate_group` directly with plain pandas -- no
SparkSession needed, since the function itself has no Spark dependency.
"""

import pandas as pd

from aastrika_telemetry.transformers.telemetry_aggregator_spark import (
    _EMPTY,
    _make_aggregate_group,
)


class _FakeAccumulator:
    """Stand-in for a pyspark Accumulator: only needs .add()."""

    def __init__(self):
        self.value = 0

    def add(self, amount):
        self.value += amount


def _events_df(rows):
    """Builds a group DataFrame shaped like what applyInPandas would pass in
    (event columns + the groupBy keys + _ingest_seq)."""
    return pd.DataFrame(rows)


def _row(eid, ets, seq, *, session_id="sess-201", content_id="content-55",
         course_id="course-9", user_id="user-100", channel="channel-web",
         platform_id="app.portal"):
    return {
        "eid": eid, "ets": ets, "_ingest_seq": seq,
        "session_id": session_id, "content_id": content_id,
        "course_id": course_id, "user_id": user_id,
        "channel": channel, "platform_id": platform_id,
    }


def test_clean_start_interact_end_group():
    acc = _FakeAccumulator()
    aggregate_group = _make_aggregate_group(acc)

    pdf = _events_df([
        _row("START", 1_000_000, 0),
        _row("INTERACT", 1_015_000, 1),
        _row("END", 1_090_000, 2),
    ])

    result = aggregate_group(pdf)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["start_ets"] == 1_000_000
    assert row["end_ets"] == 1_090_000
    assert row["total_time_duration"] == 90_000
    assert row["end_imputed"] == False
    assert row["start_imputed"] == False
    assert row["mid"] == "SESCNT_1000000_sess-201_content-55_course-9"
    assert row["channel_id"] == "channel-web"
    assert row["platform_id"] == "app.portal"
    assert acc.value == 0


def test_no_end_event_falls_back_to_last_event_and_is_imputed():
    acc = _FakeAccumulator()
    aggregate_group = _make_aggregate_group(acc)

    pdf = _events_df([
        _row("START", 1_000_000, 0),
        _row("IMPRESSION", 1_040_000, 1),
    ])

    result = aggregate_group(pdf)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["start_ets"] == 1_000_000
    assert row["end_ets"] == 1_040_000
    assert row["total_time_duration"] == 40_000
    assert row["end_imputed"] == True
    assert acc.value == 0


def test_no_start_event_discards_group():
    acc = _FakeAccumulator()
    aggregate_group = _make_aggregate_group(acc)

    pdf = _events_df([
        _row("INTERACT", 1_015_000, 0),
        _row("END", 1_090_000, 1),
    ])

    result = aggregate_group(pdf)

    assert list(result.columns) == list(_EMPTY.columns)
    assert len(result) == 0
    assert acc.value == 0  # only counted as non-positive, not as missing-START


def test_non_positive_duration_is_discarded_and_counted():
    acc = _FakeAccumulator()
    aggregate_group = _make_aggregate_group(acc)

    pdf = _events_df([
        _row("START", 1_000_000, 0),
        _row("END", 1_000_000, 1),  # same timestamp -> zero duration
    ])

    result = aggregate_group(pdf)

    assert len(result) == 0
    assert acc.value == 1


def test_end_before_start_is_ignored_falls_back_to_last_event():
    """An END with ets < start_ets must not be picked -- guards against a
    stray earlier-timestamped END producing a nonsensical negative duration."""
    acc = _FakeAccumulator()
    aggregate_group = _make_aggregate_group(acc)

    pdf = _events_df([
        _row("END", 900_000, 0),      # stray, before START -- must be ignored
        _row("START", 1_000_000, 1),
        _row("IMPRESSION", 1_020_000, 2),
    ])

    result = aggregate_group(pdf)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["start_ets"] == 1_000_000
    assert row["end_ets"] == 1_020_000  # falls back to last event, not the stray END
    assert row["end_imputed"] == True
