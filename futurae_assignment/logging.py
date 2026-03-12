import logging

from futurae_assignment.config import config


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        formatter = logging.Formatter(config.logging.format)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(config.logging.level)
        logger.propagate = False
    return logger
