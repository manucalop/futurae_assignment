from pydantic import BaseModel

from futurae_assignment.models import Event, Metrics


class EventsResponse(BaseModel):
    data: list[Event]


class MetricsResponse(BaseModel):
    data: list[Metrics]


class AggregatedMetric(BaseModel):
    service: str | None
    request_count: int
    avg_latency_ms: float
    error_rate: float
