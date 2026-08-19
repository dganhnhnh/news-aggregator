import asyncio
from pathlib import Path

import pytest
import httpx

from adapters.retrieve_news_data.google_news_rss_searcher import (
    GoogleNewsRssSearcher,
    RssFeedSource,
    _parse_rss_entry,
)
from adapters.retrieve_news_data.crawler import Crawl4aiProvider
from models.search_and_crawl_models import CrawlResult
from models.storage_models import FeedPollTarget


def test_atom_published_date_is_timezone_aware() -> None:
    entry = _parse_rss_entry(
        {
            "title": "Atom article",
            "link": "https://example.com/article",
            "published": "2026-08-01T10:30:00Z",
        }
    )

    assert entry["published_date"].isoformat() == "2026-08-01T10:30:00+00:00"


def test_load_queries_from_tsv(tmp_path: Path) -> None:
    path = tmp_path / "queries.tsv"
    path.write_text(
        "tier\tgroup\tGold market\thttps://news.google.com/rss/search?q=gold\n",
        encoding="utf-8",
    )

    assert GoogleNewsRssSearcher._load_queries_from_tsv(path) == [
        RssFeedSource(
            query_label="Gold market",
            rss_url="https://news.google.com/rss/search?q=gold",
        )
    ]


def test_load_queries_from_opml_normalizes_required_fields(tmp_path: Path) -> None:
    path = tmp_path / "feeds.opml"
    path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<opml version="2.0">
  <body>
    <outline text="Engineering Blogs">
      <outline type="rss" title="Example Engineering"
        xmlUrl="https://example.com/feed.xml"
        htmlUrl="https://example.com/blog" />
      <outline type="atom" text="Example Atom"
        xmlUrl="https://example.org/atom.xml" />
      <outline text="Folder without a feed" />
    </outline>
  </body>
</opml>
""",
        encoding="utf-8",
    )

    sources = GoogleNewsRssSearcher._load_queries_from_opml(path)

    assert sources == [
        RssFeedSource(
            query_label="Example Engineering",
            rss_url="https://example.com/feed.xml",
        ),
        RssFeedSource(
            query_label="Example Atom",
            rss_url="https://example.org/atom.xml",
        ),
    ]
    assert sources[0]._asdict() == {
        "query_label": "Example Engineering",
        "rss_url": "https://example.com/feed.xml",
    }


def test_load_queries_dispatches_by_file_extension(tmp_path: Path) -> None:
    path = tmp_path / "feeds.OPML"
    path.write_text(
        '<opml><body><outline text="Feed" xmlUrl="https://example.com/rss" />'
        "</body></opml>",
        encoding="utf-8",
    )

    assert GoogleNewsRssSearcher(feed_path=path)._load_queries() == [
        RssFeedSource(query_label="Feed", rss_url="https://example.com/rss")
    ]


def test_missing_feed_source_has_actionable_error(tmp_path: Path) -> None:
    missing = tmp_path / "missing.opml"
    searcher = GoogleNewsRssSearcher(feed_path=missing)

    with pytest.raises(FileNotFoundError, match="RSS_FEED_PATH"):
        searcher._load_queries()


def test_unsupported_feed_source_format(tmp_path: Path) -> None:
    path = tmp_path / "feeds.xml"
    path.write_text("<feeds />", encoding="utf-8")

    with pytest.raises(ValueError, match="Expected .tsv or .opml"):
        GoogleNewsRssSearcher(feed_path=path)._load_queries()


def test_opml_feed_runs_through_rss_crawl_pipeline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "feeds.opml"
    path.write_text(
        '<opml><body><outline title="Engineering Feed" '
        'xmlUrl="https://example.com/feed.xml" />'
        '<outline title="Skipped Feed" '
        'xmlUrl="https://example.org/feed.xml" /></body></opml>',
        encoding="utf-8",
    )

    class FakeResponse:
        text = """<rss version="2.0"><channel><title>Example feed</title>
        <item><title>Article</title><link>https://example.com/article</link>
        <author>Example author</author></item></channel></rss>"""
        headers: dict[str, str] = {}
        status_code = 200

        @staticmethod
        def raise_for_status() -> None:
            return None

    class FakeClient:
        async def get(self, url: str, timeout: float, headers=None):
            assert url == "https://example.com/feed.xml"
            assert timeout == 20.0
            return FakeResponse()

    async def fake_crawl_batch(self, urls, *, max_concurrency=None):
        assert max_concurrency == 2
        return [CrawlResult(url=url, markdown="# Article") for url in urls]

    monkeypatch.setattr(Crawl4aiProvider, "crawl_batch", fake_crawl_batch)
    searcher = GoogleNewsRssSearcher(
        feed_path=path,
        http_client=FakeClient(),
    )

    results = asyncio.run(
        searcher.fetch_all(max_articles_per_feed=1, max_feeds=1)
    )

    assert len(results) == 1
    assert results[0].url == "https://example.com/article"
    assert results[0].context["query_label"] == "Engineering Feed"
    assert results[0].context["feed_entry_url"] == "https://example.com/article"
    assert results[0].context["google_news_url"] == ""


def test_feed_fetch_failure_is_returned_as_structured_failure(tmp_path: Path) -> None:
    path = tmp_path / "feeds.opml"
    path.write_text(
        '<opml><body><outline title="Broken Feed" '
        'xmlUrl="https://example.com/broken.xml" /></body></opml>',
        encoding="utf-8",
    )

    class FailingClient:
        async def get(self, url: str, timeout: float, headers=None):
            request = httpx.Request("GET", url)
            raise httpx.ConnectError("connection failed", request=request)

    batch = asyncio.run(
        GoogleNewsRssSearcher(
            feed_path=path,
            http_client=FailingClient(),
        ).fetch_batch(max_articles_per_feed=1)
    )

    assert batch.feeds_processed == 1
    assert batch.results == []
    assert len(batch.failures) == 1
    assert batch.failures[0].failure_kind == "feed"
    assert batch.failures[0].url == "https://example.com/broken.xml"
    assert batch.failures[0].source == "Broken Feed"


def test_poll_feeds_sends_conditional_get_headers() -> None:
    seen_headers: dict = {}

    class FakeResponse:
        status_code = 304
        headers: dict[str, str] = {}

        @property
        def text(self) -> str:
            raise AssertionError("a 304 response must never be parsed")

        def raise_for_status(self) -> None:
            raise AssertionError("a 304 must be handled before raise_for_status")

    class FakeClient:
        async def get(self, url: str, timeout: float, headers=None):
            seen_headers.update(headers or {})
            return FakeResponse()

    target = FeedPollTarget(
        feed_url="https://example.com/feed.xml",
        source_label="Feed",
        etag='"abc123"',
        last_modified="Wed, 19 Aug 2026 00:00:00 GMT",
    )
    searcher = GoogleNewsRssSearcher(http_client=FakeClient())

    polls = asyncio.run(
        _collect(searcher.poll_feeds([target]))
    )

    assert seen_headers == {
        "If-None-Match": '"abc123"',
        "If-Modified-Since": "Wed, 19 Aug 2026 00:00:00 GMT",
    }
    assert len(polls) == 1
    assert polls[0].status == "not_modified"
    assert polls[0].etag == '"abc123"'
    assert polls[0].last_modified == "Wed, 19 Aug 2026 00:00:00 GMT"


def test_poll_feeds_reads_new_etag_and_last_modified_on_a_fresh_fetch() -> None:
    class FakeResponse:
        status_code = 200
        text = (
            "<rss><channel><title>Feed</title>"
            "<item><title>Article</title>"
            "<link>https://example.com/article</link></item>"
            "</channel></rss>"
        )
        headers = {"ETag": '"new-etag"', "Last-Modified": "Thu, 20 Aug 2026 00:00:00 GMT"}

        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        async def get(self, url: str, timeout: float, headers=None):
            return FakeResponse()

    target = FeedPollTarget(
        feed_url="https://example.com/feed.xml",
        source_label="Feed",
        etag=None,
        last_modified=None,
    )
    searcher = GoogleNewsRssSearcher(http_client=FakeClient())

    polls = asyncio.run(_collect(searcher.poll_feeds([target])))

    assert len(polls) == 1
    assert polls[0].status == "fetched"
    assert polls[0].etag == '"new-etag"'
    assert polls[0].last_modified == "Thu, 20 Aug 2026 00:00:00 GMT"
    assert [entry.entry_url for entry in polls[0].entries] == [
        "https://example.com/article"
    ]


def test_poll_feeds_follows_redirects_transparently() -> None:
    # The fake client stands in for httpx already having followed the
    # redirect (follow_redirects=True on the default client is what makes
    # that true for a real feed); the final 200 must not be treated as a
    # failure.
    class FakeResponse:
        status_code = 200
        text = "<rss><channel><title>Feed</title></channel></rss>"
        headers: dict[str, str] = {}

        def raise_for_status(self) -> None:
            return None

    class RedirectingClient:
        async def get(self, url: str, timeout: float, headers=None):
            return FakeResponse()

    target = FeedPollTarget(
        feed_url="https://example.com/feed.xml",
        source_label="Feed",
        etag=None,
        last_modified=None,
    )
    searcher = GoogleNewsRssSearcher(http_client=RedirectingClient())

    polls = asyncio.run(_collect(searcher.poll_feeds([target])))

    assert len(polls) == 1
    assert polls[0].status == "fetched"


def test_default_client_follows_redirects() -> None:
    client = GoogleNewsRssSearcher._build_default_client()
    assert client.follow_redirects is True


async def _collect(aiter):
    return [item async for item in aiter]


def test_streaming_bounds_feed_and_crawl_workers_and_rotates(
    tmp_path: Path,
) -> None:
    path = tmp_path / "feeds.opml"
    path.write_text(
        "<opml><body>"
        + "".join(
            f'<outline title="Feed {index}" '
            f'xmlUrl="https://feeds.example/{index}.xml" />'
            for index in range(3)
        )
        + "</body></opml>",
        encoding="utf-8",
    )

    class FakeResponse:
        status_code = 200
        headers: dict[str, str] = {}

        def __init__(self, feed_index: str) -> None:
            self.text = (
                "<rss><channel><title>Feed "
                + feed_index
                + "</title>"
                + "".join(
                    "<item><title>Article</title>"
                    f"<link>https://articles.example/{feed_index}/{item}</link>"
                    "</item>"
                    for item in range(5)
                )
                + "</channel></rss>"
            )

        def raise_for_status(self) -> None:
            return None

    class TrackingClient:
        active = 0
        peak = 0
        requested: list[str] = []

        async def get(self, url: str, timeout: float, headers=None):
            self.requested.append(url)
            self.active += 1
            self.peak = max(self.peak, self.active)
            await asyncio.sleep(0)
            self.active -= 1
            return FakeResponse(url.rsplit("/", 1)[-1].split(".", 1)[0])

    class TrackingProvider:
        calls: list[tuple[list[str], int | None]] = []

        async def crawl_batch(self, urls, *, max_concurrency=None):
            self.calls.append((list(urls), max_concurrency))
            return [CrawlResult(url=url, markdown="# Article") for url in urls]

    client = TrackingClient()
    provider = TrackingProvider()
    searcher = GoogleNewsRssSearcher(
        feed_path=path,
        http_client=client,
        max_concurrent_feeds=2,
        max_concurrent_crawls=2,
        crawl_batch_size=3,
        crawl_provider=provider,
    )

    async def run_twice():
        first = [
            batch
            async for batch in searcher.iter_batches(
                max_feeds=2,
                max_articles_per_feed=5,
            )
        ]
        second = [
            batch
            async for batch in searcher.iter_batches(
                max_feeds=2,
                max_articles_per_feed=1,
            )
        ]
        return first, second

    first, second = asyncio.run(run_twice())

    assert client.peak == 2
    assert all(len(batch.results) <= 3 for batch in first + second)
    assert all(concurrency == 2 for _, concurrency in provider.calls)
    assert [len(urls) for urls, _ in provider.calls[:4]] == [3, 2, 3, 2]
    assert client.requested[:2] == [
        "https://feeds.example/0.xml",
        "https://feeds.example/1.xml",
    ]
    assert client.requested[2:] == [
        "https://feeds.example/2.xml",
        "https://feeds.example/0.xml",
    ]
    assert sum(batch.crawl_succeeded for batch in first) == 10
    assert sum(batch.crawl_succeeded for batch in second) == 2
