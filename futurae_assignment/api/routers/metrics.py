from typing import Annotated

from fastapi import APIRouter, Query

from futurae_assignment.api.models import (
    AggregatedMetric,
    MetricsRequest,
    MetricsResponse,
)
from futurae_assignment.config import AppConfig
from futurae_assignment.db import DB
from futurae_assignment.models import Metrics

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("")
def list_metrics(
    db: DB,
    config: AppConfig,
    request: Annotated[MetricsRequest, Query()],
) -> MetricsResponse:
    parquet = config.pipeline.parquet_glob("metrics")
    start_date = request.start_ts.date() if request.start_ts else None
    start_hour = request.start_ts.hour if request.start_ts else None
    start_minute = request.start_ts.minute if request.start_ts else None
    end_date = request.end_ts.date() if request.end_ts else None
    end_hour = request.end_ts.hour if request.end_ts else None
    end_minute = request.end_ts.minute if request.end_ts else None

    rows = db.query(
        f"""
        select * from read_parquet('{parquet}')
        where ($1 is null or service = $1)
        and ($2 is null or (event_date::date, event_hour, event_minute) >= ($2, $3, $4))
        and ($5 is null or (event_date::date, event_hour, event_minute) <= ($5, $6, $7))
        """,  # noqa: S608
        (
            request.service,
            start_date,
            start_hour,
            start_minute,
            end_date,
            end_hour,
            end_minute,
        ),
    )
    return MetricsResponse(data=[Metrics(**row) for row in rows])


@router.get("/aggregate")
def aggregate_metrics(
    db: DB,
    config: AppConfig,
    request: Annotated[MetricsRequest, Query()],
) -> AggregatedMetric:
    parquet = config.pipeline.parquet_glob("events")
    rows = list(
        db.query(
            f"""
            select
                $1 as service
                , count(*) as request_count
                , coalesce(avg(latency_ms), 0) as avg_latency_ms
                , coalesce(
                    avg(
                        case
                            when status_code is not null
                            and (status_code < 200 or status_code > 299)
                            then 1
                            else 0
                    end)
                    , 0
                ) as error_rate
            from read_parquet('{parquet}')
            where ($1 is null or service = $1)
            and ($2 is null or event_ts >= $2)
            and ($3 is null or event_ts <= $3)
            """,  # noqa: S608
            (request.service, request.start_ts, request.end_ts),
        ),
    )
    return AggregatedMetric(**rows[0])
