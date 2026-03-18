import logging
from pathlib import Path
from typing import Optional


def setup_logging(
    log_level: int = logging.INFO, log_file: Optional[Path] = None
) -> None:
    """
    Configures the root logger for the entire project.

    This function sets up a handler for console output and, optionally,
    a handler for logging to a file. It clears any existing handlers
    to prevent duplicate messages.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Console Handler (always on)
    console_formatter = logging.Formatter("%(levelname)s [%(funcName)s]: %(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)
    root_logger.addHandler(console_handler)

    # File Handler (optional)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(module)s:%(funcName)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)  # Log more detail to the file
        root_logger.addHandler(file_handler)