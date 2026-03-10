from futurae_assignment.db import Database
from futurae_assignment.logging import get_logger

logger = get_logger(__name__)


def aggregate_metrics(
    db: Database,
    source_table_id: str,
    target_table_id: str,
) -> int:
    logger.info(f"Aggregating metrics from {source_table_id} into {target_table_id}")

    db.execute(
        f"""
        BEGIN TRANSACTION;
        TRUNCATE {target_table_id};
        INSERT INTO {target_table_id}
        SELECT
            service
            , event_ts::date AS event_date
            , hour(event_ts) AS event_hour
            , minute(event_ts) AS event_minute
            , count(*) AS request_count
            , avg(latency_ms) AS avg_latency_ms
            , avg(
                CASE WHEN status_code < 200 OR status_code > 299
                    THEN 1.0 ELSE 0.0
                END
            ) AS error_rate
            , now() AS processed_at
            , 'futurae_assignment.event_aggregator' AS processed_by
        FROM {source_table_id}
        WHERE service IS NOT NULL
        GROUP BY service, event_date, event_hour, event_minute;
        COMMIT;
        """,
    )

    result = next(db.query(f"SELECT count(*) AS total FROM {target_table_id}"))
    count = result["total"]
    logger.info(f"Loaded {count} metric rows into {target_table_id}")
    return count
