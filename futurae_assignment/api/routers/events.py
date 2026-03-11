from datetime import datetime

from fastapi import APIRouter

from futurae_assignment.api.models import EventsResponse
from futurae_assignment.config import config
from futurae_assignment.db import DB
from futurae_assignment.models import Event

router = APIRouter(prefix="/events", tags=["events"])

EVENTS_PARQUET = f"{config.pipeline.events_path}-*.parquet"


@router.get("")
def list_events(
    db: DB,
    service: str | None = None,
    event_type: str | None = None,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> EventsResponse:
    rows = db.query(
        f"""
        SELECT * FROM read_parquet('{EVENTS_PARQUET}')
        WHERE ($1 IS NULL OR service = $1)
        AND ($2 IS NULL OR event_type = $2)
        AND ($3 IS NULL OR event_ts >= $3)
        AND ($4 IS NULL OR event_ts <= $4)
        """,  # noqa: S608
        (service, event_type, start_ts, end_ts),
    )
    return EventsResponse(data=[Event(**row) for row in rows])


@router.get("/{event_id}")
def get_event(db: DB, event_id: str) -> Event | None:
    rows = list(
        db.query(
            f"""
            SELECT * FROM read_parquet('{EVENTS_PARQUET}')
            WHERE event_id = $1 LIMIT 1
            """,  # noqa: S608
            (event_id,),
        ),
    )
    if not rows:
        return None
    return Event(**rows[0])
