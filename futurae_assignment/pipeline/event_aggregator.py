from pathlib import Path

from futurae_assignment.db import Database
from futurae_assignment.logging import get_logger

logger = get_logger(__name__)


def aggregate_metrics(
    db: Database,
    dml_dir: Path,
    source_table_id: str,
    target_table_id: str,
) -> int:
    logger.info(
        f"Aggregating metrics from {source_table_id} "
        f"into {target_table_id}",
    )

    db.execute_sql_file(
        dml_dir / "aggregate_metrics.sql",
        source_table=source_table_id,
        target_table=target_table_id,
        processed_by="futurae_assignment.event_aggregator",
    )

    count = db.count(target_table_id)
    logger.info(
        f"Loaded {count} metric rows into {target_table_id}",
    )
    return count
