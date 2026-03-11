from datetime import datetime

from fastapi import APIRouter

from futurae_assignment.api.models import MetricsResponse
from futurae_assignment.config import config
from futurae_assignment.db import DB
from futurae_assignment.models import Metrics

router = APIRouter(prefix="/metrics", tags=["metrics"])

METRICS_PARQUET = f"{config.pipeline.metrics_path}-*.parquet"


@router.get("")
def list_metrics(
    db: DB,
    service: str | None = None,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> MetricsResponse:
    start_date = start_ts.date() if start_ts else None
    start_hour = start_ts.hour if start_ts else None
    start_minute = start_ts.minute if start_ts else None
    end_date = end_ts.date() if end_ts else None
    end_hour = end_ts.hour if end_ts else None
    end_minute = end_ts.minute if end_ts else None

    rows = db.query(
        f"""
        SELECT * FROM read_parquet('{METRICS_PARQUET}')
        WHERE ($1 IS NULL OR service = $1)
        AND ($2 IS NULL OR (event_date::date, event_hour, event_minute) >= ($2, $3, $4))
        AND ($5 IS NULL OR (event_date::date, event_hour, event_minute) <= ($5, $6, $7))
        """,  # noqa: S608
        (
            service,
            start_date,
            start_hour,
            start_minute,
            end_date,
            end_hour,
            end_minute,
        ),
    )
    return MetricsResponse(data=[Metrics(**row) for row in rows])
