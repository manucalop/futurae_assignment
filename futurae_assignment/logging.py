import logging

from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingSettings(BaseSettings):
    level: str = "DEBUG"
    format: str = "%(asctime)s - %(name)s - %(levelname)s: %(message)s"

    model_config = SettingsConfigDict(env_prefix="LOGGING__")


_settings = LoggingSettings()


def get_logger(name: str, settings: LoggingSettings = _settings) -> logging.Logger:
    logger = logging.getLogger(name)
    formatter = logging.Formatter(settings.format)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(settings.level)
    return logger
