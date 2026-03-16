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
    rows = db.query(
        f"""
        select * from read_parquet('{parquet}')
        where ($1 is null or service = $1)
        and ($2 is null or event_date::timestamp
            + event_hour * interval '1 hour'
            + event_minute * interval '1 minute' >= $2)
        and ($3 is null or event_date::timestamp
            + event_hour * interval '1 hour'
            + event_minute * interval '1 minute' <= $3)
        """,  # noqa: S608
        (request.service, request.start_ts, request.end_ts),
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
