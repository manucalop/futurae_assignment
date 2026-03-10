from futurae_assignment.db import Database
from futurae_assignment.logging import get_logger

logger = get_logger(__name__)


def deduplicate_events(
    db: Database,
    source_table_id: str,
    target_table_id: str,
) -> int:
    logger.info(f"Deduplicating events from {source_table_id} into {target_table_id}")

    db.execute(
        f"""
        BEGIN TRANSACTION;
        TRUNCATE {target_table_id};
        INSERT INTO {target_table_id}
        SELECT
            * EXCLUDE(_offset, processed_at, processed_by)
            , now() AS processed_at
            , 'futurae_assignment.event_deduper' AS processed_by
        FROM {source_table_id}
        QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY _offset) = 1;
        COMMIT;
        """,
    )

    result = next(db.query(f"SELECT count(*) AS total FROM {target_table_id}"))
    count = result["total"]
    logger.info(f"Loaded {count} deduplicated events into {target_table_id}")
    return count
