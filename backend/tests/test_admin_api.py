"""Unit tests for the separate operator API; no database required."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from adapters.retrieve_news_data import FeedDiscoveryError
from admin_api.deps import get_feed_discovery, get_storage
from admin_api.main import create_app
from models.discovery_models import FeedDiscoveryCandidate, FeedDiscoveryResult
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
        self.seeded: list[FeedSeed] = []
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

    async def seed_feeds(self, seeds):
        self.seeded = list(seeds)
        return sum(seed.feed_url != self.item.feed_url for seed in self.seeded)

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


class FakeDiscovery:
    def __init__(self, *, fails: bool = False) -> None:
        self.fails = fails
        self.called_with: tuple[str, str, str, int] | None = None

    async def discover(self, query, *, language, country, limit):
        self.called_with = (query, language, country, limit)
        if self.fails:
            raise FeedDiscoveryError("upstream unavailable")
        return FeedDiscoveryResult(
            query=query,
            search_feed_url="https://news.google.com/rss/search?q=ai",
            candidates=(
                FeedDiscoveryCandidate(
                    feed_url="https://example.com/feed.xml",
                    source_label="Existing",
                    website_url="https://example.com/",
                    matched_articles=3,
                    sample_titles=("One", "Two"),
                ),
                FeedDiscoveryCandidate(
                    feed_url="https://new.example/rss.xml",
                    source_label="New source",
                    website_url="https://new.example/",
                    matched_articles=1,
                    sample_titles=("Three",),
                ),
            ),
            articles_considered=4,
            publishers_considered=2,
        )


def _client(
    storage: FakeStorage | None = None,
    discovery: FakeDiscovery | None = None,
) -> tuple[TestClient, FakeStorage]:
    app = create_app()
    fake = storage or FakeStorage()
    app.dependency_overrides[get_storage] = lambda: fake
    if discovery is not None:
        app.dependency_overrides[get_feed_discovery] = lambda: discovery
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


def test_discover_feeds_marks_existing_candidates() -> None:
    discovery = FakeDiscovery()
    client, storage = _client(discovery=discovery)

    response = client.post(
        "/api/admin/feeds/discover",
        json={"query": " AI news ", "language": "vi", "country": "VN", "limit": 8},
    )

    assert response.status_code == 200
    assert discovery.called_with == ("AI news", "vi", "VN", 8)
    body = response.json()
    assert body["articles_considered"] == 4
    assert body["candidates"][0]["subscribed"] is True
    assert body["candidates"][1]["subscribed"] is False
    assert storage.list_active is None


def test_discover_feeds_maps_upstream_failure_to_bad_gateway() -> None:
    client, _ = _client(discovery=FakeDiscovery(fails=True))

    response = client.post(
        "/api/admin/feeds/discover",
        json={"query": "AI"},
    )

    assert response.status_code == 502
    assert "upstream unavailable" in response.json()["detail"]


def test_batch_create_feeds_uses_one_priority_and_ignores_duplicates() -> None:
    client, storage = _client()

    response = client.post(
        "/api/admin/feeds/batch",
        json={
            "priority": "instant",
            "feeds": [
                {
                    "feed_url": "https://new.example/rss.xml",
                    "source_label": " New source ",
                },
                {
                    "feed_url": "https://new.example/rss.xml",
                    "source_label": "Duplicate",
                },
                {"feed_url": "https://example.com/feed.xml"},
            ],
        },
    )

    assert response.status_code == 201
    assert storage.seeded == [
        FeedSeed("https://new.example/rss.xml", "New source", priority=0),
        FeedSeed("https://example.com/feed.xml", "example", priority=0),
    ]
    assert response.json() == {
        "requested_count": 2,
        "created_count": 1,
        "skipped_count": 1,
    }


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
