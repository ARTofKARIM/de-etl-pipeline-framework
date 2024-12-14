"""Centralized logging configuration for the ETL framework."""

import sys
import os
from loguru import logger


def setup_logger(
    log_dir: str = "logs",
    log_level: str = "INFO",
    rotation: str = "10 MB",
    retention: str = "30 days",
):
    """Configure application-wide logging with loguru."""
    os.makedirs(log_dir, exist_ok=True)
    logger.remove()

    # Console handler with colored output
    console_fmt = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level:<7}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    logger.add(sys.stderr, format=console_fmt, level=log_level, colorize=True)

    # File handler for all logs
    file_fmt = "{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {name}:{function}:{line} | {message}"
    logger.add(
        os.path.join(log_dir, "etl_{time}.log"),
        format=file_fmt,
        level="DEBUG",
        rotation=rotation,
        retention=retention,
        compression="zip",
    )

    # Separate error log
    logger.add(
        os.path.join(log_dir, "errors_{time}.log"),
        format=file_fmt,
        level="ERROR",
        rotation=rotation,
        retention=retention,
    )

    return logger
