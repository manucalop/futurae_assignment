from datetime import datetime

from pydantic import BaseModel

from futurae_assignment.models import Event, EventType, Metrics, Service


class EventsRequest(BaseModel):
    service: Service | None = None
    event_type: EventType | None = None
    start_ts: datetime | None = None
    end_ts: datetime | None = None


class MetricsRequest(BaseModel):
    service: Service | None = None
    start_ts: datetime | None = None
    end_ts: datetime | None = None


class EventsResponse(BaseModel):
    data: list[Event]


class MetricsResponse(BaseModel):
    data: list[Metrics]


class AggregatedMetric(BaseModel):
    service: Service | None
    request_count: int
    avg_latency_ms: float
    error_rate: float
