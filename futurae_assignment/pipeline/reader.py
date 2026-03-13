from collections.abc import Iterator
from pathlib import Path

import apache_beam as beam

from futurae_assignment.logging import get_logger

logger = get_logger(__name__)


def _get_lines(path: Path) -> Iterator[tuple[int, str]]:
    with path.open() as file:
        yield from enumerate(file)


class ReadLines(beam.PTransform):
    def __init__(self, path: Path) -> None:
        super().__init__()
        self._path = path

    def expand(self, pbegin: beam.pvalue.PBegin) -> beam.PCollection:
        logger.info("Reading lines from %s", self._path)
        return pbegin | "CreateLines" >> beam.Create(_get_lines(self._path))
