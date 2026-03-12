import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty

from futurae_assignment.pipeline.parser import INVALID_TAG, VALID_TAG, ParseEvents


def _json_line(**fields):
    defaults = {
        "event_id": "evt-1",
        "timestamp": "2025-01-15T10:30:00+00:00",
        "event_type": "request_completed",
        "service": "auth",
        "latency_ms": 100,
        "status_code": 200,
    }
    return json.dumps(defaults | fields)


def test_valid_event_is_tagged_valid():
    with TestPipeline() as p:
        results = p | beam.Create([(0, _json_line())]) | ParseEvents()
        assert_that(results[VALID_TAG], is_not_empty())


def test_invalid_json_is_tagged_invalid():
    with TestPipeline() as p:
        results = p | beam.Create([(0, "not json")]) | ParseEvents()
        assert_that(results[INVALID_TAG], is_not_empty())


def test_validation_error_is_tagged_invalid():
    with TestPipeline() as p:
        bad_record = json.dumps({"event_id": "x", "timestamp": "bad", "event_type": "request_completed"})
        results = p | beam.Create([(0, bad_record)]) | ParseEvents()
        assert_that(results[INVALID_TAG], is_not_empty())


def test_parsed_event_has_correct_fields():
    with TestPipeline() as p:
        results = p | beam.Create([(0, _json_line(event_id="evt-42"))]) | ParseEvents()
        ids = results[VALID_TAG] | beam.Map(lambda e: e.event_id)
        assert_that(ids, equal_to(["evt-42"]))
