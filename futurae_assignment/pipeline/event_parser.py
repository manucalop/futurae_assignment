import json
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from futurae_assignment.exceptions import ParsingError
from futurae_assignment.models import Event, EventStream, InvalidEvent


def get_lines(path: Path) -> Iterator[tuple[int, str]]:
    try:
        with path.open() as file:
            yield from enumerate(file)
    except FileNotFoundError as exc:
        raise ParsingError(f"File not found: {path}") from exc


def parse_events(path: Path) -> EventStream:
    for offset, line in get_lines(path):
        try:
            raw = line.rstrip("\n")
            data = json.loads(raw)
            yield Event(**data, raw=raw, offset=offset)
        except json.JSONDecodeError as exc:
            yield InvalidEvent(
                offset=offset,
                raw=raw,
                errors=[str(exc)],
            )
        except ValidationError as exc:
            yield InvalidEvent(
                offset=offset,
                raw=raw,
                errors=[e["msg"] for e in exc.errors()],
            )
