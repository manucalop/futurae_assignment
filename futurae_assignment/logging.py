import logging

from futurae_assignment.config import LoggingConfig


def get_logger(name: str, config: LoggingConfig = LoggingConfig()) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        formatter = logging.Formatter(config.format)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(config.level)
        logger.propagate = False
    return logger
