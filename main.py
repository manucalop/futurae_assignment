from futurae_assignment.config import Config
from futurae_assignment.db import Database
from futurae_assignment.pipeline.event_aggregator import aggregate_metrics
from futurae_assignment.pipeline.event_deduper import deduplicate_events
from futurae_assignment.pipeline.event_loader import load_events
from futurae_assignment.pipeline.event_parser import parse_events
from futurae_assignment.pipeline.table_creator import create_tables

config = Config()

with Database(config=config.database) as db:
    create_tables(db, ddl_dir=config.ddl_dir)
    event_stream = parse_events(path=config.data_source_file)
    load_events(
        db,
        events=event_stream,
        valid_table_id="events_valid",
        invalid_table_id="events_invalid",
    )
    deduplicate_events(
        db,
        source_table_id="events_valid",
        target_table_id="events",
    )
    aggregate_metrics(
        db,
        source_table_id="events",
        target_table_id="metrics",
    )
