from datetime import UTC, datetime

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from futurae_assignment.models import EventTuple
from futurae_assignment.pipeline.aggregator import AggregateMetrics


def _make_event_tuple(**overrides):
    defaults = dict(
        event_id="evt-1",
        event_ts=datetime(2025, 1, 15, 10, 30, tzinfo=UTC),
        service="auth",
        event_type="request_completed",
        latency_ms=100,
        status_code=200,
        user_id="u1",
        processed_at=datetime(2025, 1, 15, 10, 30, tzinfo=UTC),
        processed_by="test",
    )
    return EventTuple(**(defaults | overrides))


def test_aggregates_single_event():
    events = [_make_event_tuple()]

    with TestPipeline() as p:
        results = p | beam.Create(events) | AggregateMetrics()
        metrics = results | beam.Map(lambda m: (m.service, m.request_count, m.avg_latency_ms, m.error_rate))
        assert_that(metrics, equal_to([("auth", 1, 100.0, 0.0)]))


def test_error_rate_counts_non_2xx():
    events = [
        _make_event_tuple(event_id="1", status_code=200),
        _make_event_tuple(event_id="2", status_code=500),
    ]

    with TestPipeline() as p:
        results = p | beam.Create(events) | AggregateMetrics()
        error_rates = results | beam.Map(lambda m: m.error_rate)
        assert_that(error_rates, equal_to([0.5]))


def test_filters_events_without_service():
    events = [
        _make_event_tuple(service=None),
        _make_event_tuple(service="auth"),
    ]

    with TestPipeline() as p:
        results = p | beam.Create(events) | AggregateMetrics()
        counts = results | beam.Map(lambda m: m.request_count)
        assert_that(counts, equal_to([1]))


def test_groups_by_service():
    events = [
        _make_event_tuple(event_id="1", service="auth", latency_ms=100),
        _make_event_tuple(event_id="2", service="catalog", latency_ms=200),
    ]

    with TestPipeline() as p:
        results = p | beam.Create(events) | AggregateMetrics()
        services = results | beam.Map(lambda m: m.service)
        assert_that(services, equal_to(["auth", "catalog"]))
