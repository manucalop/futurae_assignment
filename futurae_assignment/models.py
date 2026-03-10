from collections.abc import Iterator
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, Field
from pydantic.functional_validators import BeforeValidator


def _parse_timestamp(value: Any) -> datetime:  # noqa: ANN401
    if value is None:
        return value

    if isinstance(value, datetime):
        return value

    if not isinstance(value, str):
        raise ValueError("Invalid timestamp Type")  # noqa: TRY004
        # TypeError not supported.
        # See: https://docs.pydantic.dev/latest/concepts/validators/#raising-validation-errors

    value = value.strip()
    if "/" in value:
        return datetime.strptime(value, "%d/%m/%Y %H:%M:%S").replace(tzinfo=UTC)

    return datetime.fromisoformat(value)


def _parse_latency_ms(value: Any) -> int | None:  # noqa: ANN401
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        return int(value.removesuffix("ms"))
    return value


def _parse_status_code(value: Any) -> int | None:  # noqa: ANN401
    if value is None:
        return None
    value = int(value)
    if value < 100 or value > 599:  # noqa: PLR2004
        raise AssertionError(f"status_code out of range: {value}")
    return value


class Service(StrEnum):
    AUTH = "auth"
    CATALOG = "catalog"
    CHECKOUT = "checkout"
    PAYMENTS = "payments"
    SEARCH = "search"


class EventType(StrEnum):
    REQUEST_COMPLETED = "request_completed"
    REQUEST_FAILED = "request_failed"
    REQUEST_STARTED = "request_started"


class Event(BaseModel):
    event_id: str
    event_ts: Annotated[datetime, BeforeValidator(_parse_timestamp)] = Field(
        alias="timestamp",
    )
    service: Service | None = None
    event_type: EventType
    latency_ms: Annotated[int | None, BeforeValidator(_parse_latency_ms)] = None
    status_code: Annotated[int | None, BeforeValidator(_parse_status_code)] = None
    user_id: str | None = None
    raw: str | None = None
    processed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    processed_by: str = "futurae_assignment.event_parser"
    offset: int | None = None


class InvalidEvent(BaseModel):
    raw: str
    errors: list[str]
    processed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    processed_by: str = "futurae_assignment.event_parser"
    offset: int | None = None


type EventStream = Iterator[Event | InvalidEvent]
