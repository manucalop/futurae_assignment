from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from futurae_assignment.models import Event, EventTuple, InvalidEvent


def _make_event(**overrides):
    defaults = {
        "event_id": "evt-1",
        "timestamp": "2025-01-15T10:30:00+00:00",
        "event_type": "request_completed",
        "service": "auth",
        "latency_ms": 100,
        "status_code": 200,
    }
    return Event(**(defaults | overrides))


class TestEventTimestampParsing:
    def test_iso_format(self):
        event = _make_event(timestamp="2025-01-15T10:30:00+00:00")
        assert event.event_ts == datetime(2025, 1, 15, 10, 30, tzinfo=UTC)

    def test_slash_format(self):
        event = _make_event(timestamp="15/01/2025 10:30:00")
        assert event.event_ts == datetime(2025, 1, 15, 10, 30, tzinfo=UTC)

    def test_invalid_type_raises(self):
        with pytest.raises(ValidationError):
            _make_event(timestamp=12345)


class TestEventLatencyParsing:
    def test_string_with_ms_suffix(self):
        event = _make_event(latency_ms="150ms")
        assert event.latency_ms == 150

    def test_empty_string_is_none(self):
        event = _make_event(latency_ms="")
        assert event.latency_ms is None

    def test_int_passthrough(self):
        event = _make_event(latency_ms=42)
        assert event.latency_ms == 42


class TestEventStatusCodeValidation:
    def test_valid_range(self):
        assert _make_event(status_code=200).status_code == 200
        assert _make_event(status_code=500).status_code == 500

    def test_out_of_range_raises(self):
        with pytest.raises(ValidationError):
            _make_event(status_code=99)
        with pytest.raises(ValidationError):
            _make_event(status_code=600)


class TestEventToTuple:
    def test_returns_event_tuple(self):
        event = _make_event()
        result = event.to_tuple()
        assert isinstance(result, EventTuple)
        assert result.event_id == "evt-1"
        assert result.service == "auth"
        assert result.latency_ms == 100

    def test_enum_fields_are_strings(self):
        result = _make_event().to_tuple()
        assert isinstance(result.service, str)
        assert isinstance(result.event_type, str)

    def test_none_service(self):
        result = _make_event(service=None).to_tuple()
        assert result.service is None


class TestInvalidEvent:
    def test_to_tuple_roundtrip(self):
        invalid = InvalidEvent(raw='{"bad": "json"}', errors=["missing field"], offset=5)
        t = invalid.to_tuple()
        assert t.raw == '{"bad": "json"}'
        assert t.errors == ["missing field"]
        assert t.offset == 5
