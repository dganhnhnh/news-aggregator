"""Time window shared by every worker.

Parsing lives here rather than in each worker so that `3d` means exactly the
same thing to the crawl worker and to the embed worker, and so the resolved
bounds can be recorded on the run.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


_DURATION_RE = re.compile(r"^(\d+)([mhdw])$")
_UNIT_SECONDS = {"m": 60, "h": 3600, "d": 86400, "w": 604800}
_DEFAULT_LAST = "3d"
# RSS feeds only retain their most recent entries, so a window reaching further
# back than this cannot be satisfied by discovery and deserves a warning.
DEEP_BACKFILL_THRESHOLD = timedelta(days=7)


@dataclass(frozen=True)
class RunWindow:
    start: datetime
    end: datetime
    explicit: bool
    """True when the operator named the bounds with --since/--until.

    Entries without a publication date pass an implicit window (they are
    assumed recent) but are excluded from an explicit one, because nothing
    justifies placing them inside a range the operator asked for.
    """

    @property
    def is_deep_backfill(self) -> bool:
        return self.end - self.start > DEEP_BACKFILL_THRESHOLD

    def contains(self, moment: datetime | None) -> bool:
        if moment is None:
            return not self.explicit
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return self.start <= moment <= self.end


def parse_duration(value: str) -> timedelta:
    """Parse a relative duration such as `30m`, `12h`, `3d` or `2w`."""
    match = _DURATION_RE.match(value.strip())
    if match is None:
        raise ValueError(
            f"invalid duration {value!r}; expected a number followed by "
            "m (minutes), h (hours), d (days) or w (weeks), e.g. 3d"
        )
    amount = int(match.group(1))
    if amount == 0:
        raise ValueError(f"invalid duration {value!r}; must be greater than zero")
    return timedelta(seconds=amount * _UNIT_SECONDS[match.group(2)])


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO date or datetime; naive values are read as UTC."""
    try:
        moment = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError(
            f"invalid timestamp {value!r}; expected an ISO date such as "
            "2026-08-01 or 2026-08-01T12:00:00"
        ) from exc
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment


def build_window(
    *,
    last: str | None = None,
    since: str | None = None,
    until: str | None = None,
    now: datetime | None = None,
) -> RunWindow:
    """Resolve the CLI window flags into concrete bounds."""
    end = now or datetime.now(timezone.utc)
    if since is None:
        if until is not None:
            raise ValueError("--until requires --since")
        return RunWindow(
            start=end - parse_duration(last or _DEFAULT_LAST),
            end=end,
            explicit=False,
        )
    start = parse_timestamp(since)
    if until is not None:
        end = parse_timestamp(until)
    if start >= end:
        raise ValueError(
            f"--since ({start.isoformat()}) must be earlier than "
            f"--until ({end.isoformat()})"
        )
    return RunWindow(start=start, end=end, explicit=True)
