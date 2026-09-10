"""Tests for payload projection and validation on the ingest side."""

import pytest

from pipeline.producer.produce import Stats, extract_event, serialize_key, serialize_value


def extracted(payload: dict) -> dict:
    """extract_event() for payloads a test knows are valid, narrowed for typing."""
    event = extract_event(payload)
    assert event is not None
    return event


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
        assert event is not None
        assert set(event) == {"id", "type", "title", "wiki", "user", "timestamp", "bot"}

    def test_preserves_values(self):
        event = extract_event(VALID)
        assert event is not None
        assert event["wiki"] == "enwiki"
        assert event["user"] == "SomeEditor"
        assert event["timestamp"] == 1718700000
        assert event["bot"] is False

    def test_log_events_without_an_id_are_kept(self):
        """
        Regression: Wikimedia log events (moves, deletions, user creation) have
        a null id. Requiring `id` dropped ~2.7% of the live stream and
        undercounted the "log" bucket the dashboard charts.
        """
        event = extract_event({**VALID, "id": None, "type": "log"})
        assert event is not None
        assert event["type"] == "log"
        assert event["id"] is None

    @pytest.mark.parametrize("field", ["type", "wiki"])
    def test_rejects_payloads_missing_a_required_field(self, field):
        payload = {**VALID}
        del payload[field]
        assert extract_event(payload) is None

    @pytest.mark.parametrize("field", ["type", "wiki"])
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
        assert extracted({**VALID, "bot": 1})["bot"] is True
        assert extracted({**VALID, "bot": None})["bot"] is False
        # The window keys on truthiness, so a string must not slip through.
        assert extracted({**VALID, "bot": ""})["bot"] is False

    def test_unusable_timestamp_becomes_none_rather_than_a_bad_bucket(self):
        """A string timestamp in event-time mode would silently misbucket."""
        assert extracted({**VALID, "timestamp": "2024-06-18"})["timestamp"] is None
        assert extracted({**VALID, "timestamp": None})["timestamp"] is None

    def test_wiki_is_usable_as_a_partition_key(self):
        assert isinstance(extracted(VALID)["wiki"], str)


class TestStats:
    def test_reports_only_on_the_print_interval(self, monkeypatch):
        monkeypatch.setattr("pipeline.config.PRINT_EVERY", 10)
        stats = Stats()
        for _ in range(9):
            stats.sent += 1
            assert stats.tick() is None
        stats.sent += 1
        line = stats.tick()
        assert line is not None
        assert "sent 10 events" in line

    def test_counts_start_at_zero(self):
        stats = Stats()
        assert (stats.sent, stats.dropped) == (0, 0)
        assert stats.tick() is None


class TestSerializers:
    def test_key_is_utf8_encoded(self):
        assert serialize_key("enwiki") == b"enwiki"
        assert serialize_key("zh-min-nanwiki") == b"zh-min-nanwiki"

    @pytest.mark.parametrize("bad", [None, 42, b"enwiki"])
    def test_non_string_key_fails_loudly(self, bad):
        """The kafka stubs type keys as object, so the check is ours to make."""
        with pytest.raises(TypeError, match="partition key must be str"):
            serialize_key(bad)

    def test_value_is_compact_json(self):
        assert serialize_value({"wiki": "enwiki", "bot": False}) == b'{"wiki":"enwiki","bot":false}'
