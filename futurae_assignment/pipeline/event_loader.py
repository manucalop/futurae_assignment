from jinja2 import Template

from futurae_assignment.db import Database
from futurae_assignment.logging import get_logger
from futurae_assignment.models import Event, EventStream, InvalidEvent

logger = get_logger(__name__)


def _load_valid_event(db: Database, table_id: str, e: Event) -> None:
    query = Template("""
        INSERT INTO {{ table_id }} (
            event_id
            , event_ts
            , service
            , event_type
            , latency_ms
            , status_code
            , user_id
            , raw
            , processed_at
            , processed_by
            , _offset
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """).render(table_id=table_id)
    db.execute(
        query=query,
        parameters=(
            e.event_id,
            e.event_ts,
            e.service,
            e.event_type,
            e.latency_ms,
            e.status_code,
            e.user_id,
            e.raw,
            e.processed_at,
            e.processed_by,
            e.offset,
        ),
    )


def _load_invalid_event(db: Database, table_id: str, e: InvalidEvent) -> None:
    query = Template("""
        INSERT INTO {{ table_id }} (
            raw
            , errors
            , processed_at
            , processed_by
            , _offset
        ) VALUES (?, ?, ?, ?, ?)
    """).render(table_id=table_id)
    db.execute(
        query=query,
        parameters=(
            e.raw,
            e.errors,
            e.processed_at,
            e.processed_by,
            e.offset,
        ),
    )


def load_events(
    db: Database,
    events: EventStream,
    valid_table_id: str,
    invalid_table_id: str,
) -> tuple[int, int]:
    valid_count = 0
    invalid_count = 0

    for event in events:
        match event:
            case Event():
                _load_valid_event(db, valid_table_id, event)
                valid_count += 1

            case InvalidEvent():
                _load_invalid_event(db, invalid_table_id, event)
                invalid_count += 1

    logger.info(f" {valid_count} valid events loaded into {valid_table_id}")
    logger.info(f" {invalid_count} invalid events loaded into {invalid_table_id}")
    return valid_count, invalid_count
