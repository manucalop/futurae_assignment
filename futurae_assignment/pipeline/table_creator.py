from pathlib import Path

from futurae_assignment.db import Database
from futurae_assignment.logging import get_logger

logger = get_logger(__name__)


def create_tables(db: Database, ddl_dir: Path) -> None:
    logger.info(f"Creating tables using DDL files from {ddl_dir}")
    for sql_file in sorted(ddl_dir.glob("*.sql")):
        logger.debug(f"Executing DDL file: {sql_file}")
        db.execute(sql_file.read_text())

    logger.info("Tables created successfully")
