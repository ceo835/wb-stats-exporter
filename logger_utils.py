"""Logging utilities for Streamlit app and background jobs."""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_FILE = LOG_DIR / "app.log"
LOGGER_NAME = "wb_stats"


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure application logger with file rotation and console output."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.propagate = False

    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def tail_log_lines(lines: int = 10) -> list[str]:
    """Return last N lines of the main log file."""
    if not LOG_FILE.exists():
        return ["Лог-файл пока не создан."]

    try:
        with LOG_FILE.open("r", encoding="utf-8") as file:
            content = file.readlines()
        return [line.rstrip("\n") for line in content[-max(lines, 1) :]]
    except OSError as exc:
        return [f"Не удалось прочитать лог: {exc}"]


def get_log_level(default: str = "INFO") -> str:
    """Read log level from environment."""
    return os.getenv("LOG_LEVEL", default).strip() or default
