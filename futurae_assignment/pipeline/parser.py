import json
from collections.abc import Iterator

import apache_beam as beam
from pydantic import ValidationError

from futurae_assignment.logging import get_logger
from futurae_assignment.models import Event, InvalidEvent

logger = get_logger(__name__)

VALID_TAG = "valid"
INVALID_TAG = "invalid"


class _ValidateEvent(beam.DoFn):
    def process(  # ty: ignore[invalid-method-override]
        self,
        element: tuple[int, str],
    ) -> Iterator[beam.pvalue.TaggedOutput]:
        offset, raw = element
        try:
            record = json.loads(raw)
            event = Event(**record, offset=offset)
            yield beam.pvalue.TaggedOutput(VALID_TAG, event.to_tuple())

        except json.JSONDecodeError as exc:
            logger.debug("JSON decode error at offset %d: %s", offset, exc)
            invalid = InvalidEvent(raw=raw, errors=[str(exc)], offset=offset)
            yield beam.pvalue.TaggedOutput(INVALID_TAG, invalid.to_tuple())

        except ValidationError as exc:
            errors = [str(e) for e in exc.errors()]
            logger.debug("Validation error at offset %d: %s", offset, errors)
            invalid = InvalidEvent(raw=raw, errors=errors, offset=offset)
            yield beam.pvalue.TaggedOutput(INVALID_TAG, invalid.to_tuple())


class ParseEvents(beam.PTransform):
    def expand(self, pcoll: beam.PCollection):  # noqa: ANN201
        return pcoll | "Validate" >> beam.ParDo(_ValidateEvent()).with_outputs(
            VALID_TAG,
            INVALID_TAG,
        )
