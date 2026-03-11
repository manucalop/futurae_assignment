from pathlib import Path

from pydantic import BaseModel, FilePath
from pydantic_settings import BaseSettings, SettingsConfigDict

root_path = Path(__file__).parent.parent


class LoggingConfig(BaseModel):
    level: str = "DEBUG"
    format: str = "%(asctime)s - %(name)s - %(levelname)s: %(message)s"


class DatabaseConfig(BaseModel):
    path: Path = root_path / "data" / "output" / "warehouse.duckdb"


class BeamPipelineConfig(BaseModel):
    input_path: FilePath = root_path / "data" / "input" / "HomeAssignmentEvents.jsonl"
    output_dir: Path = root_path / "data" / "output"

    @property
    def events_valid_path(self) -> Path:
        return self.output_dir / "events_valid"

    @property
    def events_invalid_path(self) -> Path:
        return self.output_dir / "events_invalid"

    @property
    def events_path(self) -> Path:
        return self.output_dir / "events"

    @property
    def metrics_path(self) -> Path:
        return self.output_dir / "metrics"


class Config(BaseSettings):
    logging: LoggingConfig = LoggingConfig()
    database: DatabaseConfig = DatabaseConfig()
    pipeline: BeamPipelineConfig = BeamPipelineConfig()

    model_config = SettingsConfigDict(env_nested_delimiter="__")


config = Config()
