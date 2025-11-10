"""Logging configuration."""

import logging
import sys
from rich.logging import RichHandler


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Setup logger with rich handler.
    
    Args:
        name: Logger name
        level: Logging level
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Rich handler for beautiful console output
    handler = RichHandler(
        rich_tracebacks=True,
        markup=True,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    
    logger.addHandler(handler)
    
    return logger