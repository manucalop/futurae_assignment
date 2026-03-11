from pydantic import BaseModel

from futurae_assignment.models import Event, Metrics


class EventsResponse(BaseModel):
    data: list[Event]


class MetricsResponse(BaseModel):
    data: list[Metrics]
