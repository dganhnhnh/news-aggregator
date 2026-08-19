"""Deprecated alias for `news-worker crawl`.

Kept so existing cron entries and shell scripts keep working. Every flag has
the same name under `news-worker crawl` except `--interval-seconds`, which is
now `--interval`.
"""

from __future__ import annotations

import sys

from core.config import Settings
from core.logger import get_logger, setup_logging
from workers import cli as worker_cli


logger = get_logger(__name__)

_INTERVAL_FLAG = "--interval-seconds"


def translate_argv(argv: list[str], default_interval: int) -> list[str]:
    """Turn legacy news-service arguments into news-worker crawl arguments.

    Beyond renaming `--interval-seconds`, this restores the old default: bare
    `news-service` looped forever, whereas bare `news-worker crawl` runs once.
    """
    once = "--once" in argv
    translated: list[str] = ["crawl"]
    skip_next = False
    saw_interval = False
    for index, argument in enumerate(argv):
        if skip_next:
            skip_next = False
            continue
        name, separator, value = argument.partition("=")
        if name != _INTERVAL_FLAG:
            translated.append(argument)
            continue
        saw_interval = True
        if once:
            # --once won out under the old parser; the new one rejects both.
            skip_next = not separator and index + 1 < len(argv)
            continue
        translated.append("--interval")
        if separator:
            translated.append(value)
    if not once and not saw_interval:
        translated += ["--interval", str(default_interval)]
    return translated


def main() -> None:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    logger.warning(
        "news-service is deprecated and will be removed; "
        "use `news-worker crawl` instead (--interval-seconds is now --interval)"
    )
    worker_cli.main(
        translate_argv(sys.argv[1:], settings.service_interval_seconds)
    )


if __name__ == "__main__":
    main()
