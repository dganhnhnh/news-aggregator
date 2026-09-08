"""Unit tests for the separate operator API; no database required."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from admin_api.deps import get_storage
from admin_api.main import create_app
from models.storage_models import FeedSeed, FeedStateItem


NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)


def _item(
    feed_url: str = "https://example.com/feed.xml",
    *,
    source_label: str = "example",
    priority: int = 1,
    active: bool = True,
) -> FeedStateItem:
    return FeedStateItem(
        feed_url=feed_url,
        source_label=source_label,
        priority=priority,
        resolved_url=None,
        last_polled_at=None,
        last_success_at=None,
        next_poll_at=NOW,
        consecutive_failures=0,
        disabled_at=None if active else NOW,
        disabled_reason=None if active else "manual",
        updated_at=NOW,
    )


class FakeStorage:
    def __init__(self) -> None:
        self.item = _item()
        self.list_active: bool | None = True
        self.created_seed: FeedSeed | None = None
        self.updated: tuple[str, int | None, bool | None] | None = None

    async def list_feeds(self, *, active=True):
        self.list_active = active
        return [self.item]

    async def create_feed(self, seed: FeedSeed):
        self.created_seed = seed
        if seed.feed_url == self.item.feed_url:
            return None
        return _item(
            seed.feed_url,
            source_label=seed.source_label,
            priority=seed.priority,
        )

    async def update_feed(self, feed_url, *, priority=None, active=None):
        self.updated = (feed_url, priority, active)
        if feed_url == "https://missing.example/feed.xml":
            return None
        result = self.item
        if priority is not None:
            result = replace(result, priority=priority)
        if active is not None:
            result = replace(
                result,
                disabled_at=None if active else NOW,
                disabled_reason=None if active else "Disabled from feed management UI",
            )
        return result


def _client(storage: FakeStorage | None = None) -> tuple[TestClient, FakeStorage]:
    app = create_app()
    fake = storage or FakeStorage()
    app.dependency_overrides[get_storage] = lambda: fake
    return TestClient(app), fake


def test_list_all_feeds_maps_storage_priority_and_status() -> None:
    client, storage = _client()
    response = client.get("/api/admin/feeds?status=all")

    assert response.status_code == 200
    assert storage.list_active is None
    assert response.json()[0]["priority"] == "normal"
    assert response.json()[0]["active"] is True


def test_create_feed_uses_explicit_label_and_instant_priority() -> None:
    client, storage = _client()
    response = client.post(
        "/api/admin/feeds",
        json={
            "feed_url": "https://feeds.other.example/rss",
            "source_label": " Other ",
            "priority": "instant",
        },
    )

    assert response.status_code == 201
    assert storage.created_seed == FeedSeed(
        "https://feeds.other.example/rss", "Other", priority=0
    )
    assert response.json()["priority"] == "instant"


def test_create_feed_infers_registrable_domain_label() -> None:
    client, storage = _client()
    response = client.post(
        "/api/admin/feeds",
        json={
            "feed_url": "https://feeds.bbc.co.uk/news/rss.xml",
            "source_label": "  ",
            "priority": "normal",
        },
    )

    assert response.status_code == 201
    assert storage.created_seed == FeedSeed(
        "https://feeds.bbc.co.uk/news/rss.xml", "bbc", priority=1
    )


def test_create_duplicate_feed_is_conflict() -> None:
    client, _ = _client()
    response = client.post(
        "/api/admin/feeds",
        json={"feed_url": "https://example.com/feed.xml", "priority": "normal"},
    )
    assert response.status_code == 409
    assert response.json() == {"detail": "Feed URL already exists"}


def test_create_feed_rejects_invalid_url() -> None:
    client, _ = _client()
    response = client.post(
        "/api/admin/feeds",
        json={"feed_url": "ftp://example.com/feed.xml", "priority": "normal"},
    )
    assert response.status_code == 422


def test_update_feed_priority_and_active_status() -> None:
    client, storage = _client()
    response = client.patch(
        "/api/admin/feeds",
        json={
            "feed_url": "https://example.com/feed.xml",
            "priority": "instant",
            "active": False,
        },
    )
    assert response.status_code == 200
    assert storage.updated == ("https://example.com/feed.xml", 0, False)
    assert response.json()["priority"] == "instant"
    assert response.json()["active"] is False


def test_update_feed_requires_a_change() -> None:
    client, _ = _client()
    response = client.patch(
        "/api/admin/feeds", json={"feed_url": "https://example.com/feed.xml"}
    )
    assert response.status_code == 422


def test_update_missing_feed_is_not_found() -> None:
    client, _ = _client()
    response = client.patch(
        "/api/admin/feeds",
        json={"feed_url": "https://missing.example/feed.xml", "active": True},
    )
    assert response.status_code == 404
