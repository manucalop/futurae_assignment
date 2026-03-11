from typing import Any

import apache_beam as beam

from futurae_assignment.logging import get_logger
from futurae_assignment.models import Metrics

logger = get_logger(__name__)

MetricsKey = tuple[str, str, int, int]
MetricsAccumulator = tuple[int, int, int]


def _metrics_key(event: dict[str, Any]) -> MetricsKey:
    ts = event["event_ts"]
    return (str(event["service"]), str(ts.date()), ts.hour, ts.minute)


class _AggregateCombineFn(beam.CombineFn):
    def create_accumulator(self) -> MetricsAccumulator:
        return (0, 0, 0)

    def add_input(
        self,
        accumulator: MetricsAccumulator,
        event: dict[str, Any],
    ) -> MetricsAccumulator:
        count, latency_sum, error_count = accumulator
        latency = event.get("latency_ms") or 0
        status = event.get("status_code")
        is_error = int(status is not None and (status < 200 or status > 299))  # noqa: PLR2004
        return (count + 1, latency_sum + latency, error_count + is_error)

    def merge_accumulators(
        self,
        accumulators: list[MetricsAccumulator],
    ) -> MetricsAccumulator:
        return (
            sum(a[0] for a in accumulators),
            sum(a[1] for a in accumulators),
            sum(a[2] for a in accumulators),
        )

    def extract_output(
        self,
        accumulator: MetricsAccumulator,
    ) -> MetricsAccumulator:
        return accumulator


def _to_metric_row(
    element: tuple[MetricsKey, MetricsAccumulator],
) -> dict[str, Any]:
    key, acc = element
    service, event_date, event_hour, event_minute = key
    count, latency_sum, error_count = acc
    return Metrics(
        service=service,
        event_date=event_date,
        event_hour=event_hour,
        event_minute=event_minute,
        request_count=count,
        avg_latency_ms=latency_sum / count if count else 0.0,
        error_rate=error_count / count if count else 0.0,
    ).model_dump()


class AggregateMetrics(beam.PTransform):
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        logger.info("Aggregating metrics by service/date/hour/minute")
        return (
            pcoll
            | "FilterWithService" >> beam.Filter(lambda e: e.get("service") is not None)
            | "KeyByMetricsBucket" >> beam.Map(lambda e: (_metrics_key(e), e))
            | "Aggregate" >> beam.CombinePerKey(_AggregateCombineFn())
            | "ToMetricRows" >> beam.Map(_to_metric_row)
        )
