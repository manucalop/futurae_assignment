from datetime import datetime

from fastapi import APIRouter

from futurae_assignment.api.models import EventsResponse
from futurae_assignment.config import AppConfig
from futurae_assignment.db import DB
from futurae_assignment.models import Event

router = APIRouter(prefix="/events", tags=["events"])


@router.get("")
def list_events(
    db: DB,
    cfg: AppConfig,
    service: str | None = None,
    event_type: str | None = None,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> EventsResponse:
    parquet = cfg.pipeline.parquet_glob("events")
    rows = db.query(
        f"""
        select * from read_parquet('{parquet}')
        where ($1 is null or service = $1)
        and ($2 is null or event_type = $2)
        and ($3 is null or event_ts >= $3)
        and ($4 is null or event_ts <= $4)
        """,  # noqa: S608
        (service, event_type, start_ts, end_ts),
    )
    return EventsResponse(data=[Event(**row) for row in rows])


@router.get("/{event_id}")
def get_event(db: DB, cfg: AppConfig, event_id: str) -> Event | None:
    parquet = cfg.pipeline.parquet_glob("events")
    rows = list(
        db.query(
            f"""
            select * from read_parquet('{parquet}')
            where event_id = $1 limit 1
            """,  # noqa: S608
            (event_id,),
        ),
    )
    if not rows:
        return None
    return Event(**rows[0])
