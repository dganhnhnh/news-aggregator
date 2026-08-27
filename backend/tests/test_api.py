"""Unit tests for the news-api FastAPI app; no database required."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from api.cursor import decode_cursor, encode_cursor
from api.deps import get_storage
from api.main import create_app
from models.storage_models import (
    ArticleDetailItem,
    ArticleFeedItem,
    SourceItem,
    StoryDetailItem,
    StoryFeedItem,
    StoryMember,
    StorySourceRef,
)


class FakeStorage:
    def __init__(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self._feed_items = [
            ArticleFeedItem(
                id=1,
                url="https://example.com/a",
                title="Article A",
                excerpt="full markdown body",
                source_slug="example.com",
                source_name="Example",
                published_at=now,
                crawled_at=now,
                event_at=now,
            )
        ]
        self._detail = ArticleDetailItem(
            url="https://example.com/a",
            url_original="https://example.com/a",
            title="Article A",
            content="full markdown body",
            source_slug="example.com",
            source_name="Example",
            published_at=now,
            crawled_at=now,
            event_at=now,
        )
        self._sources = [SourceItem(slug="example.com", name="Example", homepage=None)]
        self.listed_sources_since = None
        self.listed_sources_until = None
        self._story_items = [
            StoryFeedItem(
                id=10,
                headline="Story A",
                first_seen_at=now,
                last_seen_at=now,
                article_count=2,
                source_count=1,
                sources=[StorySourceRef(slug="example.com", name="Example")],
            )
        ]
        self._story_detail = StoryDetailItem(
            id=10,
            headline="Story A",
            first_seen_at=now,
            last_seen_at=now,
            article_count=2,
            source_count=1,
            members=[
                StoryMember(
                    article_id=1,
                    title="Article A",
                    url="https://example.com/a",
                    source_slug="example.com",
                    source_name="Example",
                    published_at=now,
                    event_at=now,
                )
            ],
        )
        self.list_stories_source_slugs = None

    async def feed_articles(
        self, *, since, until, limit, cursor=None, source_slugs=None, query=None, max_per_source=3
    ):
        return self._feed_items[:limit]

    async def source_articles(self, slug, *, since, until, limit, cursor=None, query=None):
        if slug != "example.com":
            return None
        return self._feed_items[:limit]

    async def list_sources(self, *, since=None, until=None):
        self.listed_sources_since = since
        self.listed_sources_until = until
        return self._sources

    async def get_article(self, article_id):
        return self._detail if article_id == 1 else None

    async def list_stories(
        self,
        *,
        since,
        until,
        limit,
        cursor=None,
        source_slugs=None,
        min_sources=1,
        min_articles=1,
    ):
        self.list_stories_source_slugs = source_slugs
        return self._story_items[:limit]

    async def get_story(self, story_id):
        return self._story_detail if story_id == 10 else None


def _client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_storage] = FakeStorage
    return TestClient(app)


def test_health() -> None:
    client = _client()
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_list_articles() -> None:
    client = _client()
    response = client.get("/api/articles?limit=10")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert "window" in body
    assert body["items"] == [
        {
            "id": 1,
            "url": "https://example.com/a",
            "title": "Article A",
            "excerpt": "full markdown body",
            "source": {"slug": "example.com", "name": "Example"},
            "published_at": "2026-01-01T00:00:00Z",
            "crawled_at": "2026-01-01T00:00:00Z",
            "event_at": "2026-01-01T00:00:00Z",
        }
    ]


def test_list_articles_invalid_cursor_is_400() -> None:
    client = _client()
    response = client.get("/api/articles?cursor=not-a-valid-cursor!!!")
    assert response.status_code == 400


def test_get_article_found() -> None:
    client = _client()
    response = client.get("/api/articles/1")
    assert response.status_code == 200
    assert response.json()["content"] == "full markdown body"


def test_get_article_not_found() -> None:
    client = _client()
    response = client.get("/api/articles/999")
    assert response.status_code == 404


def test_list_sources() -> None:
    client = _client()
    response = client.get("/api/sources")
    assert response.status_code == 200
    assert response.json() == [{"slug": "example.com", "name": "Example", "homepage": None}]


def test_list_sources_passes_since_filter() -> None:
    app = create_app()
    storage = FakeStorage()
    app.dependency_overrides[get_storage] = lambda: storage
    client = TestClient(app)

    response = client.get(
        "/api/sources?since=2026-01-01T00:00:00Z&until=2026-02-01T00:00:00Z"
    )

    assert response.status_code == 200
    assert storage.listed_sources_since == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert storage.listed_sources_until == datetime(2026, 2, 1, tzinfo=timezone.utc)


def test_source_articles_not_found() -> None:
    client = _client()
    response = client.get("/api/sources/no-such-source/articles")
    assert response.status_code == 404


def test_list_stories() -> None:
    client = _client()
    response = client.get("/api/stories?limit=10")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"] == [
        {
            "id": 10,
            "headline": "Story A",
            "first_seen_at": "2026-01-01T00:00:00Z",
            "last_seen_at": "2026-01-01T00:00:00Z",
            "article_count": 2,
            "source_count": 1,
            "sources": [{"slug": "example.com", "name": "Example"}],
        }
    ]


def test_list_stories_passes_source_filter() -> None:
    app = create_app()
    storage = FakeStorage()
    app.dependency_overrides[get_storage] = lambda: storage
    client = TestClient(app)

    response = client.get("/api/stories?source=example.com&source=other.com")

    assert response.status_code == 200
    assert storage.list_stories_source_slugs == ["example.com", "other.com"]


def test_list_stories_invalid_cursor_is_400() -> None:
    client = _client()
    response = client.get("/api/stories?cursor=not-a-valid-cursor!!!")
    assert response.status_code == 400


def test_get_story_found() -> None:
    client = _client()
    response = client.get("/api/stories/10")
    assert response.status_code == 200
    body = response.json()
    assert body["headline"] == "Story A"
    assert body["members"] == [
        {
            "article_id": 1,
            "title": "Article A",
            "url": "https://example.com/a",
            "source": {"slug": "example.com", "name": "Example"},
            "published_at": "2026-01-01T00:00:00Z",
            "event_at": "2026-01-01T00:00:00Z",
        }
    ]


def test_get_story_not_found() -> None:
    client = _client()
    response = client.get("/api/stories/999")
    assert response.status_code == 404


def test_cursor_round_trip() -> None:
    event_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    decoded = decode_cursor(encode_cursor(event_at, 42))
    assert decoded == (event_at, 42)
