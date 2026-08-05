"""Small logging setup shared by adapters and entrypoints."""

from __future__ import annotations

import logging
import os
import sys


def setup_logging(level: str | None = None) -> None:
    resolved_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=getattr(logging, resolved_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stderr,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
