from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import app
from futurae_assignment.config import (
    BeamPipelineConfig,
    Config,
    DatabaseConfig,
    get_config,
)
from futurae_assignment.models import Event


@pytest.fixture
def events_parquet(tmp_path):
    table = pa.table(
        {
            "event_id": ["e1", "e2", "e3"],
            "event_ts": [
                datetime(2025, 1, 15, 10, 0, tzinfo=UTC),
                datetime(2025, 1, 15, 10, 5, tzinfo=UTC),
                datetime(2025, 1, 15, 11, 0, tzinfo=UTC),
            ],
            "service": ["auth", "auth", "catalog"],
            "event_type": ["request_completed", "request_completed", "request_failed"],
            "latency_ms": [100, 200, None],
            "status_code": [200, 500, 503],
            "user_id": ["u1", "u2", "u3"],
            "processed_at": [datetime(2025, 1, 15, tzinfo=UTC)] * 3,
            "processed_by": ["test"] * 3,
        },
        schema=Event.arrow_schema(),
    )
    pq.write_table(table, str(tmp_path / "events-00000.parquet"))
    return tmp_path


@pytest.fixture
def client(events_parquet):
    test_config = Config(
        pipeline=BeamPipelineConfig(
            input_path=Path(__file__),
            output_dir=events_parquet,
        ),
        database=DatabaseConfig(path=events_parquet / "test.duckdb"),
    )

    app.dependency_overrides[get_config] = lambda: test_config
    yield TestClient(app)
    app.dependency_overrides = {}
