from pathlib import Path

from pydantic import BaseModel, DirectoryPath, FilePath
from pydantic_settings import BaseSettings, SettingsConfigDict

root_path = Path(__file__).parent.parent


class LoggingConfig(BaseModel):
    level: str = "DEBUG"
    format: str = "%(asctime)s - %(name)s - %(levelname)s: %(message)s"


class DatabaseConfig(BaseModel):
    path: FilePath = root_path / Path("data/output.db")


class Config(BaseSettings):
    logging: LoggingConfig = LoggingConfig()
    database: DatabaseConfig = DatabaseConfig()
    data_source_file: FilePath = root_path / Path("data/HomeAssignmentEvents.jsonl")
    ddl_dir: DirectoryPath = root_path / Path("sql/ddl")

    model_config = SettingsConfigDict(env_nested_delimiter="__")


config = Config()
