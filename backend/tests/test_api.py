"""Unit tests for the news-api FastAPI app; no database required."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from api.deps import get_storage
from api.main import create_app
from models.storage_models import ArticleListItem, RawArticle


class FakeStorage:
    def __init__(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self._articles = {
            1: RawArticle(
                url="https://example.com/a",
                url_original="https://example.com/a",
                title="Article A",
                source="Example",
                lang="en",
                content="full markdown body",
                content_hash=b"1" * 32,
                published_at=now,
                crawled_at=now,
            )
        }
        self._list = [
            ArticleListItem(
                id=1,
                url="https://example.com/a",
                title="Article A",
                source="Example",
                published_at=now,
                crawled_at=now,
            )
        ]

    async def list_articles(self, *, limit, offset, source=None, query=None):
        return self._list[offset : offset + limit], len(self._list)

    async def get_article(self, article_id):
        return self._articles.get(article_id)


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
    response = client.get("/api/articles?limit=10&offset=0")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["limit"] == 10
    assert body["offset"] == 0
    assert body["items"] == [
        {
            "id": 1,
            "url": "https://example.com/a",
            "title": "Article A",
            "source": "Example",
            "published_at": "2026-01-01T00:00:00Z",
            "crawled_at": "2026-01-01T00:00:00Z",
        }
    ]


def test_get_article_found() -> None:
    client = _client()
    response = client.get("/api/articles/1")
    assert response.status_code == 200
    assert response.json()["content"] == "full markdown body"


def test_get_article_not_found() -> None:
    client = _client()
    response = client.get("/api/articles/999")
    assert response.status_code == 404
