from pathlib import Path

from futurae_assignment.db import Database
from futurae_assignment.logging import get_logger

logger = get_logger(__name__)


def deduplicate_events(
    db: Database,
    dml_dir: Path,
    source_table_id: str,
    target_table_id: str,
) -> int:
    logger.info(
        f"Deduplicating events from {source_table_id} "
        f"into {target_table_id}",
    )

    db.execute_sql_file(
        dml_dir / "deduplicate_events.sql",
        source_table=source_table_id,
        target_table=target_table_id,
        processed_by="futurae_assignment.event_deduper",
    )

    count = db.count(target_table_id)
    logger.info(
        f"Loaded {count} deduplicated events into {target_table_id}",
    )
    return count
