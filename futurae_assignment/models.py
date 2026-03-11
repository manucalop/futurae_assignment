from collections.abc import Iterator
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field
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
    model_config = ConfigDict(populate_by_name=True)

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
    processed_by: str = "futurae_assignment.pipeline"
    offset: int | None = None

    @classmethod
    def arrow_schema(cls) -> pa.Schema:
        return pa.schema(
            [
                ("event_id", pa.string()),
                ("event_ts", pa.timestamp("us", tz="UTC")),
                ("service", pa.string()),
                ("event_type", pa.string()),
                ("latency_ms", pa.int64()),
                ("status_code", pa.int64()),
                ("user_id", pa.string()),
                ("raw", pa.string()),
                ("processed_at", pa.timestamp("us", tz="UTC")),
                ("processed_by", pa.string()),
                ("offset", pa.int64()),
            ],
        )


class InvalidEvent(BaseModel):
    raw: str
    errors: list[str]
    processed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    processed_by: str = "futurae_assignment.pipeline"
    offset: int | None = None

    @classmethod
    def arrow_schema(cls) -> pa.Schema:
        return pa.schema(
            [
                ("raw", pa.string()),
                ("errors", pa.list_(pa.string())),
                ("processed_at", pa.timestamp("us", tz="UTC")),
                ("processed_by", pa.string()),
                ("offset", pa.int64()),
            ],
        )


type EventStream = Iterator[Event | InvalidEvent]


class Metrics(BaseModel):
    service: str
    event_date: str
    event_hour: int
    event_minute: int
    request_count: int
    avg_latency_ms: float
    error_rate: float
    processed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    processed_by: str = "futurae_assignment.pipeline"

    @classmethod
    def arrow_schema(cls) -> pa.Schema:
        return pa.schema(
            [
                ("service", pa.string()),
                ("event_date", pa.string()),
                ("event_hour", pa.int64()),
                ("event_minute", pa.int64()),
                ("request_count", pa.int64()),
                ("avg_latency_ms", pa.float64()),
                ("error_rate", pa.float64()),
                ("processed_at", pa.timestamp("us", tz="UTC")),
                ("processed_by", pa.string()),
            ],
        )
