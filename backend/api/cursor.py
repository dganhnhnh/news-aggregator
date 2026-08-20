"""Opaque keyset cursors for `(event_at, id)`-ordered article feeds."""

from __future__ import annotations

import base64
from datetime import datetime

from fastapi import HTTPException


def encode_cursor(event_at: datetime, article_id: int) -> str:
    raw = f"{event_at.isoformat()}|{article_id}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        event_at_raw, article_id_raw = raw.rsplit("|", 1)
        return datetime.fromisoformat(event_at_raw), int(article_id_raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc
