from typing import NamedTuple

import apache_beam as beam

from futurae_assignment.logging import get_logger
from futurae_assignment.models import EventTuple, Metrics, MetricsTuple

logger = get_logger(__name__)


class MetricsKey(NamedTuple):
    service: str
    event_date: str
    event_hour: int
    event_minute: int


class MetricsAccumulator(NamedTuple):
    count: int
    latency_sum: int
    error_count: int


def _metrics_key(event: EventTuple) -> MetricsKey:
    return MetricsKey(
        service=str(event.service),
        event_date=str(event.event_ts.date()),
        event_hour=event.event_ts.hour,
        event_minute=event.event_ts.minute,
    )


class _AggregateCombineFn(beam.CombineFn):
    def create_accumulator(self) -> MetricsAccumulator:
        return MetricsAccumulator(count=0, latency_sum=0, error_count=0)

    def add_input(
        self,
        accumulator: MetricsAccumulator,
        event: EventTuple,
    ) -> MetricsAccumulator:
        count, latency_sum, error_count = accumulator
        latency = event.latency_ms or 0
        status = event.status_code
        is_error = int(status is not None and (status < 200 or status > 299))  # noqa: PLR2004
        return MetricsAccumulator(
            count=count + 1,
            latency_sum=latency_sum + latency,
            error_count=error_count + is_error,
        )

    def merge_accumulators(
        self,
        accumulators: list[MetricsAccumulator],
    ) -> MetricsAccumulator:
        return MetricsAccumulator(
            count=sum(a.count for a in accumulators),
            latency_sum=sum(a.latency_sum for a in accumulators),
            error_count=sum(a.error_count for a in accumulators),
        )

    def extract_output(
        self,
        accumulator: MetricsAccumulator,
    ) -> MetricsAccumulator:
        return accumulator


def _to_metric_row(
    element: tuple[MetricsKey, MetricsAccumulator],
) -> MetricsTuple:
    key, acc = element
    return Metrics(
        service=key.service,
        event_date=key.event_date,
        event_hour=key.event_hour,
        event_minute=key.event_minute,
        request_count=acc.count,
        avg_latency_ms=acc.latency_sum / acc.count if acc.count else 0.0,
        error_rate=acc.error_count / acc.count if acc.count else 0.0,
    ).to_tuple()


class AggregateMetrics(beam.PTransform):
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        logger.info("Aggregating metrics by service/date/hour/minute")
        return (
            pcoll
            | "FilterWithService" >> beam.Filter(lambda e: e.service is not None)
            | "KeyByMetricsBucket" >> beam.Map(lambda e: (_metrics_key(e), e))
            | "Aggregate" >> beam.CombinePerKey(_AggregateCombineFn())
            | "ToMetricRows" >> beam.Map(_to_metric_row)
        )
