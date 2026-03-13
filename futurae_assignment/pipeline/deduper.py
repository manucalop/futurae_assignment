import apache_beam as beam
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

from futurae_assignment.logging import get_logger
from futurae_assignment.models import EventTuple

logger = get_logger(__name__)


def _stamp_event_time(event: EventTuple) -> TimestampedValue:
    return TimestampedValue(event, Timestamp.from_utc_datetime(event.event_ts))


class DeduplicateEvents(beam.PTransform):
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        logger.info("Deduplicating events by event_id (keeping latest)")
        return (
            pcoll
            | "StampEventTime" >> beam.Map(_stamp_event_time)
            | "KeyByEventId" >> beam.Map(lambda e: (e.event_id, e))
            | "KeepLatest" >> beam.combiners.Latest.PerKey()
            | "DropKeys" >> beam.Values()
        )
