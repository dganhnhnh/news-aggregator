import asyncio
from pathlib import Path

import pytest

from adapters.retrieve_news_data.google_news_rss_searcher import (
    GoogleNewsRssSearcher,
    RssFeedSource,
)
from adapters.retrieve_news_data.crawler import Crawl4aiProvider
from models.search_and_crawl_models import CrawlResult


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

        @staticmethod
        def raise_for_status() -> None:
            return None

    class FakeClient:
        async def get(self, url: str, timeout: float):
            assert url == "https://example.com/feed.xml"
            assert timeout == 20.0
            return FakeResponse()

    async def fake_crawl(self, url: str) -> CrawlResult:
        return CrawlResult(url=url, markdown="# Article")

    monkeypatch.setattr(Crawl4aiProvider, "crawl", fake_crawl)
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
