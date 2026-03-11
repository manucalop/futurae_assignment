import shutil

import apache_beam as beam
from apache_beam.io.parquetio import WriteToParquet

from futurae_assignment.config import config
from futurae_assignment.logging import get_logger
from futurae_assignment.models import Event, InvalidEvent, Metrics
from futurae_assignment.pipeline.aggregator import AggregateMetrics
from futurae_assignment.pipeline.deduper import DeduplicateEvents
from futurae_assignment.pipeline.parser import INVALID_TAG, VALID_TAG, ParseEvents
from futurae_assignment.pipeline.reader import ReadLines

logger = get_logger(__name__)


def run() -> None:
    cfg = config.pipeline

    if cfg.output_dir.exists():
        shutil.rmtree(cfg.output_dir)
    cfg.output_dir.mkdir(parents=True)

    logger.info(
        "Starting pipeline, input=%s, output=%s",
        cfg.input_path,
        cfg.output_dir,
    )

    with beam.Pipeline() as pipeline:
        lines = pipeline | "ReadJSONL" >> ReadLines(cfg.input_path)
        results = lines | "Parse" >> ParseEvents()
        deduplicated = results[VALID_TAG] | "Deduplicate" >> DeduplicateEvents()
        metrics = deduplicated | "AggregateMetrics" >> AggregateMetrics()

        results[VALID_TAG] | "WriteValidEvents" >> WriteToParquet(
            str(cfg.events_valid_path),
            schema=Event.arrow_schema(),
            file_name_suffix=".parquet",
        )
        results[INVALID_TAG] | "WriteInvalidEvents" >> WriteToParquet(
            str(cfg.events_invalid_path),
            schema=InvalidEvent.arrow_schema(),
            file_name_suffix=".parquet",
        )

        deduplicated | "WriteEvents" >> WriteToParquet(
            str(cfg.events_path),
            schema=Event.arrow_schema(),
            file_name_suffix=".parquet",
        )

        metrics | "WriteMetrics" >> WriteToParquet(
            str(cfg.metrics_path),
            schema=Metrics.arrow_schema(),
            file_name_suffix=".parquet",
        )

    logger.info("Pipeline finished")
