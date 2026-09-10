"""Tests for the aggregator's pure helpers. Sink behaviour lives in test_sink.py."""

import pytest
from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata

from pipeline.consumer.aggregator import build_commit, event_timestamp, format_summary
from pipeline.consumer.window import TumblingWindow


def window(start, total, bot=0):
    w = TumblingWindow(start=start, size=5)
    for i in range(total):
        w.add({"wiki": "enwiki", "user": "alice", "bot": i < bot, "type": "edit"})
    return w.snapshot()


# ---------------------------------------------------------------------------
# Time source
# ---------------------------------------------------------------------------

class TestEventTimestamp:
    def test_arrival_mode_ignores_the_payload_clock(self):
        assert event_timestamp({"timestamp": 50}, arrival=999.0, source="arrival") == 999.0

    def test_event_mode_uses_the_payload_clock(self):
        assert event_timestamp({"timestamp": 50}, arrival=999.0, source="event") == 50.0

    @pytest.mark.parametrize("bad", [None, 0, -1, "2024-01-01", {}])
    def test_event_mode_falls_back_when_the_payload_clock_is_unusable(self, bad):
        assert event_timestamp({"timestamp": bad}, arrival=999.0, source="event") == 999.0

    def test_missing_field_falls_back_to_arrival(self):
        assert event_timestamp({}, arrival=999.0, source="event") == 999.0


# ---------------------------------------------------------------------------
# Console rendering
# ---------------------------------------------------------------------------

class TestFormatSummary:
    def test_reports_totals_and_bot_share(self):
        line = format_summary(window(100, 10, bot=4))
        assert "10 edits" in line
        assert "2.0/s" in line
        assert "bots: 40%" in line

    def test_empty_window_does_not_divide_by_zero(self):
        line = format_summary(window(100, 0))
        assert "bots: 0%" in line
        assert "-" in line


# ---------------------------------------------------------------------------
# Offset commit payload
# ---------------------------------------------------------------------------

class TestBuildCommit:
    """
    Regression: OffsetTracker returns plain ints, but KafkaConsumer.commit()
    asserts on OffsetAndMetadata and fails at runtime, not at import.
    """

    def test_values_are_offset_and_metadata_structs(self):
        commit = build_commit({("wiki-edits", 0): 42})
        assert all(isinstance(v, OffsetAndMetadata) for v in commit.values())

    def test_keys_are_topic_partitions(self):
        commit = build_commit({("wiki-edits", 3): 7})
        assert list(commit) == [TopicPartition("wiki-edits", 3)]

    def test_offset_is_preserved(self):
        commit = build_commit({("wiki-edits", 0): 42})
        assert next(iter(commit.values())).offset == 42

    def test_struct_matches_the_installed_arity(self):
        """kafka-python 2.2 added leader_epoch with no default."""
        value = next(iter(build_commit({("t", 0): 1}).values()))
        assert len(value) == len(OffsetAndMetadata._fields)

    def test_empty_input_yields_empty_commit(self):
        assert build_commit({}) == {}

    def test_multiple_partitions_are_all_included(self):
        commit = build_commit({("t", 0): 1, ("t", 1): 2, ("t", 2): 3})
        assert len(commit) == 3
        assert {tp.partition for tp in commit} == {0, 1, 2}
