"""Unit tests for the news-api FastAPI app; no database required."""

from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi.testclient import TestClient

from api.cursor import decode_cursor, encode_cursor
from api.deps import get_storage
from api.main import create_app
from models.storage_models import (
    ArticleDetailItem,
    ArticleFeedItem,
    DailyArticleCount,
    SourceItem,
    StoryDetailItem,
    StoryFeedItem,
    StoryMember,
    StorySourceRef,
    TaxonomyCatalogItem,
    TaxonomyNodeSnapshot,
    TopicCatalogItem,
    TopicRefItem,
)


class FakeStorage:
    def __init__(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        topic = TopicRefItem(id=7, slug="artificial-intelligence", display_name="AI")
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
                topics=(topic,),
            )
        ]
        self._detail = ArticleDetailItem(
            url="https://example.com/a",
            url_original="https://example.com/a",
            title="Article A",
            content="full markdown body",
            feed_summary="Article A summary",
            source_slug="example.com",
            source_name="Example",
            published_at=now,
            crawled_at=now,
            event_at=now,
            topics=(topic,),
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
                topics=(topic,),
            )
        ]
        self._story_detail = StoryDetailItem(
            id=10,
            headline="Story A",
            first_seen_at=now,
            last_seen_at=now,
            article_count=2,
            source_count=1,
            topics=(topic,),
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
        # Three day shapes in one series: a partial edge day, a day the
        # crawler produced nothing at all, and an ordinary day.
        self._trend_days = (
            DailyArticleCount(day=date(2026, 1, 1), matches=2, articles=5, partial=True),
            DailyArticleCount(day=date(2026, 1, 2), matches=0, articles=0, partial=False),
            DailyArticleCount(day=date(2026, 1, 3), matches=1, articles=4, partial=False),
        )
        self.trend_call = None
        self.list_stories_source_slugs = None
        self.list_stories_topic_slugs = None
        self.feed_article_topic_slugs = None
        self._topic_catalog = TaxonomyCatalogItem(
            slug="default",
            display_name="Default",
            revision=2,
            topics=(
                TopicCatalogItem(
                    id=7,
                    slug="artificial-intelligence",
                    display_name="AI",
                    description="Models and tools",
                ),
            ),
            nodes=(
                TaxonomyNodeSnapshot(
                    slug="technology",
                    display_name="Technology",
                    sort_order=0,
                    topic_slugs=["artificial-intelligence"],
                    children=[],
                ),
            ),
        )

    async def feed_articles(
        self,
        *,
        since,
        until,
        limit,
        cursor=None,
        source_slugs=None,
        topic_slugs=None,
        query=None,
        max_per_source=3,
    ):
        self.feed_article_topic_slugs = topic_slugs
        return self._feed_items[:limit]

    async def daily_article_counts(
        self, *, since, until, query, source_slugs=None, topic_slugs=None
    ):
        self.trend_call = {
            "since": since,
            "until": until,
            "query": query,
            "source_slugs": source_slugs,
            "topic_slugs": topic_slugs,
        }
        return self._trend_days

    async def source_articles(
        self, slug, *, since, until, limit, cursor=None, topic_slugs=None, query=None
    ):
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
        topic_slugs=None,
        min_sources=1,
        min_articles=1,
    ):
        self.list_stories_source_slugs = source_slugs
        self.list_stories_topic_slugs = topic_slugs
        return self._story_items[:limit]

    async def get_story(self, story_id):
        return self._story_detail if story_id == 10 else None

    async def get_taxonomy_catalog(self, slug="default"):
        return self._topic_catalog if slug == "default" else None


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
            "title_highlights": None,
            "excerpt": "full markdown body",
            "excerpt_highlights": None,
            "source": {"slug": "example.com", "name": "Example"},
            "published_at": "2026-01-01T00:00:00Z",
            "crawled_at": "2026-01-01T00:00:00Z",
            "event_at": "2026-01-01T00:00:00Z",
            "topics": [{"id": 7, "slug": "artificial-intelligence", "name": "AI"}],
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
    assert response.json()["feed_summary"] == "Article A summary"
    assert response.json()["topics"] == [
        {"id": 7, "slug": "artificial-intelligence", "name": "AI"}
    ]


def test_list_articles_passes_topic_filter() -> None:
    app = create_app()
    storage = FakeStorage()
    app.dependency_overrides[get_storage] = lambda: storage
    client = TestClient(app)

    response = client.get("/api/articles?topic=artificial-intelligence&topic=open-source")

    assert response.status_code == 200
    assert storage.feed_article_topic_slugs == ["artificial-intelligence", "open-source"]


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
            "topics": [{"id": 7, "slug": "artificial-intelligence", "name": "AI"}],
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


def test_list_stories_passes_topic_filter() -> None:
    app = create_app()
    storage = FakeStorage()
    app.dependency_overrides[get_storage] = lambda: storage
    client = TestClient(app)

    response = client.get("/api/stories?topic=artificial-intelligence&topic=open-source")

    assert response.status_code == 200
    assert storage.list_stories_topic_slugs == ["artificial-intelligence", "open-source"]


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
    assert body["topics"] == [
        {"id": 7, "slug": "artificial-intelligence", "name": "AI"}
    ]
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


def test_get_topics_returns_catalog_and_tree() -> None:
    client = _client()
    response = client.get("/api/topics?taxonomy=default")

    assert response.status_code == 200
    assert response.json() == {
        "taxonomy": {"slug": "default", "name": "Default", "revision": 2},
        "topics": [
            {
                "id": 7,
                "slug": "artificial-intelligence",
                "name": "AI",
                "description": "Models and tools",
            }
        ],
        "nodes": [
            {
                "slug": "technology",
                "name": "Technology",
                "topic_slugs": ["artificial-intelligence"],
                "children": [],
            }
        ],
    }


def test_get_topics_unknown_taxonomy_is_404() -> None:
    client = _client()
    response = client.get("/api/topics?taxonomy=missing")
    assert response.status_code == 404


def test_cursor_round_trip() -> None:
    event_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    decoded = decode_cursor(encode_cursor(event_at, 42))
    assert decoded == (event_at, 42)


def test_trends_returns_a_dense_series_with_totals() -> None:
    client = _client()
    response = client.get(
        "/api/trends?q=ai&since=2026-01-01T00:00:00Z&until=2026-01-04T00:00:00Z"
    )
    assert response.status_code == 200
    assert response.json() == {
        "query": "ai",
        "window": {
            "since": "2026-01-01T00:00:00Z",
            "until": "2026-01-04T00:00:00Z",
        },
        # Summed from the buckets, never counted independently.
        "totals": {"matches": 3, "articles": 9},
        "days": [
            {"day": "2026-01-01", "matches": 2, "articles": 5, "partial": True},
            # A day with no articles at all survives as a zero row rather than
            # dropping out, so the chart can draw it as a crawl gap.
            {"day": "2026-01-02", "matches": 0, "articles": 0, "partial": False},
            {"day": "2026-01-03", "matches": 1, "articles": 4, "partial": False},
        ],
    }


def test_trends_requires_a_non_blank_query() -> None:
    client = _client()
    assert client.get("/api/trends").status_code == 422
    assert client.get("/api/trends?q=%20%20").status_code == 400


def test_trends_passes_source_and_topic_filters() -> None:
    # The regression guard for "Trends and Articles can never disagree": both
    # filters have to reach storage, or the two views answer different
    # questions about the same query.
    app = create_app()
    storage = FakeStorage()
    app.dependency_overrides[get_storage] = lambda: storage
    client = TestClient(app)

    response = client.get("/api/trends?q=ai&source=a&source=b&topic=open-source")

    assert response.status_code == 200
    assert storage.trend_call is not None
    assert storage.trend_call["source_slugs"] == ["a", "b"]
    assert storage.trend_call["topic_slugs"] == ["open-source"]
    assert storage.trend_call["query"] == "ai"


def test_trends_rejects_an_oversized_window() -> None:
    client = _client()
    response = client.get(
        "/api/trends?q=ai&since=2020-01-01T00:00:00Z&until=2026-01-01T00:00:00Z"
    )
    assert response.status_code == 400
    assert "maximum" in response.json()["detail"]


def test_trends_rejects_an_inverted_window() -> None:
    client = _client()
    response = client.get(
        "/api/trends?q=ai&since=2026-01-04T00:00:00Z&until=2026-01-01T00:00:00Z"
    )
    assert response.status_code == 400


def test_trends_treats_a_naive_since_as_utc() -> None:
    # A bound with no offset must not be read in the host's local time: the
    # day buckets are computed from these values, so an implicit local zone
    # would shift every bar.
    app = create_app()
    storage = FakeStorage()
    app.dependency_overrides[get_storage] = lambda: storage
    client = TestClient(app)

    response = client.get("/api/trends?q=ai&since=2026-01-01T00:00:00")

    assert response.status_code == 200
    assert storage.trend_call is not None
    assert storage.trend_call["since"] == datetime(2026, 1, 1, tzinfo=timezone.utc)
