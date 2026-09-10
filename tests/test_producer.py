"""Tests for payload projection and validation on the ingest side."""

import pytest

from pipeline.producer.produce import Stats, extract_event

VALID = {
    "id": 12345678,
    "type": "edit",
    "title": "Python (programming language)",
    "wiki": "enwiki",
    "user": "SomeEditor",
    "timestamp": 1718700000,
    "bot": False,
    "comment": "dropped",
    "server_url": "dropped",
}


class TestExtractEvent:
    def test_projects_only_the_fields_we_aggregate(self):
        event = extract_event(VALID)
        assert set(event) == {"id", "type", "title", "wiki", "user", "timestamp", "bot"}

    def test_preserves_values(self):
        event = extract_event(VALID)
        assert event["wiki"] == "enwiki"
        assert event["user"] == "SomeEditor"
        assert event["timestamp"] == 1718700000
        assert event["bot"] is False

    @pytest.mark.parametrize("field", ["id", "type", "wiki"])
    def test_rejects_payloads_missing_a_required_field(self, field):
        payload = {**VALID}
        del payload[field]
        assert extract_event(payload) is None

    @pytest.mark.parametrize("field", ["id", "type", "wiki"])
    def test_rejects_empty_required_fields(self, field):
        assert extract_event({**VALID, field: None}) is None
        assert extract_event({**VALID, field: ""}) is None

    @pytest.mark.parametrize("payload", [None, [], "edit", 42])
    def test_rejects_non_dict_payloads(self, payload):
        assert extract_event(payload) is None

    def test_optional_fields_may_be_absent(self):
        event = extract_event({"id": 1, "type": "log", "wiki": "dewiki"})
        assert event is not None
        assert event["user"] is None
        assert event["title"] is None

    def test_bot_flag_is_coerced_to_bool(self):
        assert extract_event({**VALID, "bot": 1})["bot"] is True
        assert extract_event({**VALID, "bot": None})["bot"] is False
        # The window keys on truthiness, so a string must not slip through.
        assert extract_event({**VALID, "bot": ""})["bot"] is False

    def test_unusable_timestamp_becomes_none_rather_than_a_bad_bucket(self):
        """A string timestamp in event-time mode would silently misbucket."""
        assert extract_event({**VALID, "timestamp": "2024-06-18"})["timestamp"] is None
        assert extract_event({**VALID, "timestamp": None})["timestamp"] is None

    def test_wiki_is_usable_as_a_partition_key(self):
        assert isinstance(extract_event(VALID)["wiki"], str)


class TestStats:
    def test_reports_only_on_the_print_interval(self, monkeypatch):
        monkeypatch.setattr("pipeline.config.PRINT_EVERY", 10)
        stats = Stats()
        for _ in range(9):
            stats.sent += 1
            assert stats.tick() is None
        stats.sent += 1
        assert "sent 10 events" in stats.tick()

    def test_counts_start_at_zero(self):
        stats = Stats()
        assert (stats.sent, stats.dropped) == (0, 0)
        assert stats.tick() is None
