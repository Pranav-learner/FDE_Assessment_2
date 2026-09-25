"""Logging configuration layer for NYC Taxi Operations Intelligence Pipeline.

Provides structured logging to console and persistent log files in logs/
with standard levels: INFO (normal flow), WARNING (data quality), ERROR (failures).
"""

import logging
from pathlib import Path
import sys
from typing import Optional


def get_logger(
    name: str = "nyc_taxi_pipeline",
    log_file: Optional[Path | str] = None,
    level: int = logging.INFO,
    clear_handlers: bool = False,
) -> logging.Logger:
    """Configure and return a structured logger.

    Args:
        name: Name of the logger.
        log_file: Optional file path for file logging. If None, defaults
            to project_root / 'logs' / 'pipeline.log'.
        level: Base logging level (defaults to INFO).
        clear_handlers: If True, clears existing handlers before adding new ones.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if clear_handlers:
        logger.handlers.clear()

    # Avoid duplicate handlers if already configured and clear_handlers was False
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)
    logger.addHandler(console_handler)

    # File Handler
    if log_file is not None:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(file_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)
    else:
        # Default project log location
        project_root = Path(__file__).resolve().parent.parent
        default_log_dir = project_root / "logs"
        default_log_dir.mkdir(parents=True, exist_ok=True)
        default_log_file = default_log_dir / "pipeline.log"
        file_handler = logging.FileHandler(default_log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

    return logger
