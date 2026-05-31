"""Centralized logging configuration for the Hydra Data Factory pipeline."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Final

LOG_FORMAT: Final[str] = (
    "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s"
)
DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
APP_LOGGER_NAME: Final[str] = "hydra"
DEFAULT_LOG_DIR: Final[Path] = Path("logs")
DEFAULT_LOG_FILE: Final[Path] = DEFAULT_LOG_DIR / "pipeline.log"

_configured: bool = False


def setup_logger(
    name: str = APP_LOGGER_NAME,
    level: int = logging.INFO,
    log_dir: Path | None = None,
) -> logging.Logger:
    """
    Configure the root application logger and return it.

    Logs are emitted to stdout and to ``logs/pipeline.log`` (created if absent).
    Child loggers obtained via :func:`get_logger` propagate to this root logger.
    """
    global _configured

    logger = logging.getLogger(name)

    if _configured and logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    target_dir = log_dir or DEFAULT_LOG_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    log_file = target_dir / "pipeline.log"

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    _configured = True
    logger.debug("Logger initialized; writing to %s", log_file.resolve())

    return logger


def get_logger(name: str = APP_LOGGER_NAME) -> logging.Logger:
    """
    Return a module-scoped logger that propagates to the Hydra root logger.

    Pass ``__name__`` from calling modules so log records include the correct
    source filename via the logging hierarchy.
    """
    if not _configured:
        setup_logger()

    if name == APP_LOGGER_NAME or name.startswith(f"{APP_LOGGER_NAME}."):
        return logging.getLogger(name)

    return logging.getLogger(f"{APP_LOGGER_NAME}.{name}")
