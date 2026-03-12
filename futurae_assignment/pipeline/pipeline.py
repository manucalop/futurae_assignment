import shutil

import apache_beam as beam

from futurae_assignment.config import BeamPipelineConfig
from futurae_assignment.logging import get_logger
from futurae_assignment.models import Event, InvalidEvent, Metrics
from futurae_assignment.pipeline.aggregator import AggregateMetrics
from futurae_assignment.pipeline.deduper import DeduplicateEvents
from futurae_assignment.pipeline.parser import INVALID_TAG, VALID_TAG, ParseEvents
from futurae_assignment.pipeline.reader import ReadLines
from futurae_assignment.pipeline.writer import WriteParquet

logger = get_logger(__name__)


def run(config: BeamPipelineConfig) -> None:

    if config.output_dir.exists():
        shutil.rmtree(config.output_dir)
    config.output_dir.mkdir(parents=True)

    logger.info(
        "Starting pipeline, input=%s, output=%s",
        config.input_path,
        config.output_dir,
    )

    with beam.Pipeline() as pipeline:
        lines = pipeline | "ReadJSONL" >> ReadLines(config.input_path)
        results = lines | "Parse" >> ParseEvents()
        deduplicated = results[VALID_TAG] | "Deduplicate" >> DeduplicateEvents()
        metrics = deduplicated | "AggregateMetrics" >> AggregateMetrics()

        results[VALID_TAG] | "WriteValidEvents" >> WriteParquet(
            str(config.events_valid_path),
            Event.arrow_schema(),
        )
        results[INVALID_TAG] | "WriteInvalidEvents" >> WriteParquet(
            str(config.events_invalid_path),
            InvalidEvent.arrow_schema(),
        )
        deduplicated | "WriteEvents" >> WriteParquet(
            str(config.events_path),
            Event.arrow_schema(),
        )
        metrics | "WriteMetrics" >> WriteParquet(
            str(config.metrics_path),
            Metrics.arrow_schema(),
        )

    logger.info("Pipeline finished")
