import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest
import httpx

from adapters.retrieve_news_data.google_news_rss_searcher import (
    GoogleNewsRssSearcher,
    RssFeedSource,
    _ArticleJob,
    _parse_rss_entry,
)
from core.config import DEFAULT_USER_AGENT
from adapters.retrieve_news_data.crawler import Crawl4aiProvider
from models.discovery_models import DiscoveredEntry
from models.search_and_crawl_models import CrawlMetadata, CrawlResult
from models.storage_models import FeedPollTarget


class _Redirect:
    """One hop of httpx's `response.history`."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


def test_atom_published_date_is_timezone_aware() -> None:
    entry = _parse_rss_entry(
        {
            "title": "Atom article",
            "link": "https://example.com/article",
            "published": "2026-08-01T10:30:00Z",
        }
    )

    assert entry["published_date"].isoformat() == "2026-08-01T10:30:00+00:00"


@pytest.mark.parametrize(
    "link, expected",
    [
        # Root-relative, as served by several GitHub Pages engineering blogs.
        ("/2020/06/22/post.html", "https://blog.example/2020/06/22/post.html"),
        # Protocol-relative.
        ("//blog.example/devops/the-dojo", "https://blog.example/devops/the-dojo"),
        # Path-relative: the bug that produced `https://tech.finn.no2018/...`.
        ("2018/06/28/hack-days/", "https://blog.example/2018/06/28/hack-days/"),
        # Already absolute: untouched.
        ("https://other.example/post", "https://other.example/post"),
    ],
)
def test_entry_links_are_resolved_against_the_feed_url(
    link: str, expected: str
) -> None:
    entry = _parse_rss_entry({"link": link}, "https://blog.example/feed.xml")

    assert entry["feed_entry_url"] == expected


def test_entry_link_keeps_its_own_host_when_the_feed_redirected() -> None:
    entry = _parse_rss_entry(
        {"link": "/post"}, "https://moved.example/feeds/main.xml"
    )

    assert entry["feed_entry_url"] == "https://moved.example/post"


def test_podcast_enclosure_is_not_treated_as_an_article() -> None:
    entry = _parse_rss_entry(
        {
            "title": "Episode 12",
            "links": [
                {
                    "href": "https://dcs.megaphone.fm/GLT4653461142.mp3?key=abc",
                    "rel": "enclosure",
                    "type": "audio/mpeg",
                }
            ],
        },
        "https://feeds.megaphone.fm/show",
    )

    assert entry["feed_entry_url"] == ""


def test_episode_page_is_preferred_over_its_audio_enclosure() -> None:
    entry = _parse_rss_entry(
        {
            "title": "Episode 12",
            "links": [
                {
                    "href": "https://dcs.megaphone.fm/GLT4653461142.mp3",
                    "rel": "enclosure",
                    "type": "audio/mpeg",
                },
                {
                    "href": "https://show.example/episodes/12",
                    "rel": "alternate",
                    "type": "text/html",
                },
            ],
        },
        "https://feeds.megaphone.fm/show",
    )

    assert entry["feed_entry_url"] == "https://show.example/episodes/12"


def test_media_link_in_the_link_element_is_rejected() -> None:
    # Some Megaphone feeds put the audio in <link> itself, where there is no
    # enclosure rel to filter on.
    entry = _parse_rss_entry(
        {"link": "https://dcs-spotify.megaphone.fm/GLT5843461507.mp3?key=abc"},
        "https://feeds.megaphone.fm/show",
    )

    assert entry["feed_entry_url"] == ""


def test_non_http_entry_link_is_rejected() -> None:
    entry = _parse_rss_entry(
        {"link": "mailto:editor@example.com"}, "https://blog.example/feed.xml"
    )

    assert entry["feed_entry_url"] == ""


def test_entries_without_an_article_link_are_dropped_and_counted() -> None:
    class FakeResponse:
        status_code = 200
        headers: dict[str, str] = {}
        url = "https://feeds.megaphone.fm/show"
        history: list = []
        text = """<rss version="2.0"><channel><title>Show</title>
        <item><title>Episode 12</title>
        <enclosure url="https://dcs.megaphone.fm/a.mp3" type="audio/mpeg" /></item>
        <item><title>Show notes</title>
        <link>https://show.example/notes/12</link></item>
        </channel></rss>"""

        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        async def get(self, url: str, timeout: float, headers=None):
            return FakeResponse()

    target = FeedPollTarget(
        feed_url="https://feeds.megaphone.fm/show",
        source_label="Show",
        etag=None,
        last_modified=None,
    )

    polls = asyncio.run(
        _collect(GoogleNewsRssSearcher(http_client=FakeClient()).poll_feeds([target]))
    )

    assert [entry.entry_url for entry in polls[0].entries] == [
        "https://show.example/notes/12"
    ]
    assert polls[0].entries_skipped_non_article == 1


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


def test_google_news_source_url_is_preserved_for_feed_discovery() -> None:
    parsed = _parse_rss_entry(
        {
            "title": "Story",
            "link": "https://news.google.com/rss/articles/token",
            "source": {
                "title": "Publisher",
                "href": "https://publisher.example/news",
            },
        }
    )

    assert parsed["source"] == "Publisher"
    assert parsed["source_url"] == "https://publisher.example/news"


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
        url = "https://example.com/feed.xml"
        history: list = []

        @staticmethod
        def raise_for_status() -> None:
            return None

    class FakeClient:
        async def get(self, url: str, timeout: float, headers=None):
            assert url == "https://example.com/feed.xml"
            assert timeout == 8.0
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
        url = "https://example.com/feed.xml"
        history: list = []

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
        cadence_checked_at=datetime(2026, 8, 19, tzinfo=timezone.utc),
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


def test_cadence_bootstrap_omits_existing_conditional_headers() -> None:
    seen_headers: list[dict | None] = []

    class FakeResponse:
        status_code = 200
        text = "<rss><channel><title>Feed</title></channel></rss>"
        headers: dict[str, str] = {}
        url = "https://example.com/feed.xml"
        history: list = []

        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        async def get(self, url: str, timeout: float, headers=None):
            seen_headers.append(headers)
            return FakeResponse()

    target = FeedPollTarget(
        feed_url="https://example.com/feed.xml",
        source_label="Feed",
        etag='"old-etag"',
        last_modified="Wed, 19 Aug 2026 00:00:00 GMT",
    )

    polls = asyncio.run(
        _collect(GoogleNewsRssSearcher(http_client=FakeClient()).poll_feeds([target]))
    )

    assert polls[0].status == "fetched"
    assert seen_headers == [None]


def test_poll_feeds_uses_configured_fetch_timeout() -> None:
    seen_timeouts: list[float] = []

    class FakeResponse:
        status_code = 200
        text = "<rss><channel><title>Feed</title></channel></rss>"
        headers: dict[str, str] = {}
        url = "https://example.com/feed.xml"
        history: list = []

        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        async def get(self, url: str, timeout: float, headers=None):
            seen_timeouts.append(timeout)
            return FakeResponse()

    target = FeedPollTarget("https://example.com/feed.xml", "Feed", None, None)
    searcher = GoogleNewsRssSearcher(
        http_client=FakeClient(), feed_fetch_timeout_seconds=3.5
    )

    asyncio.run(_collect(searcher.poll_feeds([target])))

    assert seen_timeouts == [3.5]


def test_poll_feeds_refills_a_finished_slot_before_a_slow_fetch_finishes() -> None:
    class FakeResponse:
        status_code = 200
        headers: dict[str, str] = {}
        history: list = []

        def __init__(self, url: str) -> None:
            self.url = url
            self.text = "<rss><channel><title>Feed</title></channel></rss>"

        def raise_for_status(self) -> None:
            return None

    class ControlledClient:
        def __init__(self) -> None:
            self.release_slow = asyncio.Event()
            self.next_started = asyncio.Event()

        async def get(self, url: str, timeout: float, headers=None):
            if url.endswith("slow.xml"):
                await self.release_slow.wait()
            if url.endswith("next.xml"):
                self.next_started.set()
            return FakeResponse(url)

    async def scenario() -> list:
        client = ControlledClient()
        targets = [
            FeedPollTarget("https://example.com/slow.xml", "slow", None, None),
            FeedPollTarget("https://example.com/fast.xml", "fast", None, None),
            FeedPollTarget("https://example.com/next.xml", "next", None, None),
        ]
        collecting = asyncio.create_task(
            _collect(
                GoogleNewsRssSearcher(
                    http_client=client, max_concurrent_feeds=2
                ).poll_feeds(targets)
            )
        )
        await asyncio.wait_for(client.next_started.wait(), timeout=1)
        await asyncio.sleep(0)
        client.release_slow.set()
        return await collecting

    polls = asyncio.run(scenario())

    assert [poll.target.source_label for poll in polls] == ["fast", "next", "slow"]


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
        url = "https://example.com/feed.xml"
        history: list = []

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
    # failure, and a permanent chain is reported so the caller can record it.
    class FakeResponse:
        status_code = 200
        text = "<rss><channel><title>Feed</title></channel></rss>"
        headers: dict[str, str] = {}
        url = "https://example.com/moved/feed.xml"
        history = [_Redirect(301)]

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
    assert polls[0].resolved_url == "https://example.com/moved/feed.xml"


def test_poll_feeds_does_not_record_a_temporary_redirect_as_the_new_home() -> None:
    class FakeResponse:
        status_code = 200
        text = "<rss><channel><title>Feed</title></channel></rss>"
        headers: dict[str, str] = {}
        url = "https://example.com/temporary/feed.xml"
        history = [_Redirect(302)]

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

    assert polls[0].status == "fetched"
    assert polls[0].resolved_url is None


def test_poll_feeds_polls_the_resolved_url_when_one_is_known() -> None:
    requested: list[str] = []

    class FakeResponse:
        status_code = 200
        text = "<rss><channel><title>Feed</title></channel></rss>"
        headers: dict[str, str] = {}
        url = "https://example.com/moved/feed.xml"
        history: list = []

        def raise_for_status(self) -> None:
            return None

    class RecordingClient:
        async def get(self, url: str, timeout: float, headers=None):
            requested.append(url)
            return FakeResponse()

    target = FeedPollTarget(
        feed_url="https://example.com/feed.xml",
        source_label="Feed",
        etag=None,
        last_modified=None,
        resolved_url="https://example.com/moved/feed.xml",
    )
    searcher = GoogleNewsRssSearcher(http_client=RecordingClient())

    polls = asyncio.run(_collect(searcher.poll_feeds([target])))

    assert requested == ["https://example.com/moved/feed.xml"]
    # The row keeps its identity from the feed file, and nothing new to record.
    assert polls[0].target.feed_url == "https://example.com/feed.xml"
    assert polls[0].resolved_url is None


@pytest.mark.parametrize(
    "status, permanent",
    [(404, True), (410, True), (401, True), (403, False), (429, False), (500, False)],
)
def test_permanent_feed_statuses_are_flagged_for_immediate_disabling(
    status: int, permanent: bool
) -> None:
    class FailingClient:
        async def get(self, url: str, timeout: float, headers=None):
            request = httpx.Request("GET", url)
            response = httpx.Response(status, request=request)
            response.raise_for_status()

    target = FeedPollTarget(
        feed_url="https://example.com/feed.xml",
        source_label="Feed",
        etag=None,
        last_modified=None,
    )
    searcher = GoogleNewsRssSearcher(http_client=FailingClient())

    polls = asyncio.run(_collect(searcher.poll_feeds([target])))

    assert polls[0].status == "failed"
    assert polls[0].error.permanent is permanent


def test_a_permanent_redirect_survives_a_failure_at_the_new_url() -> None:
    # Slashdot's shape: 301 to https, then 403 from a bot filter. The 403 must
    # not throw away the redirect, or every poll re-pays for the same hop and
    # the stale http:// URL never leaves the rotation.
    class RedirectThenForbidden:
        async def get(self, url: str, timeout: float, headers=None):
            request = httpx.Request("GET", "https://example.com/feed.xml")
            response = httpx.Response(403, request=request)
            response.history = [_Redirect(301)]
            response.raise_for_status()

    target = FeedPollTarget(
        feed_url="http://example.com/feed.xml",
        source_label="Feed",
        etag=None,
        last_modified=None,
    )
    searcher = GoogleNewsRssSearcher(http_client=RedirectThenForbidden())

    polls = asyncio.run(_collect(searcher.poll_feeds([target])))

    assert polls[0].status == "failed"
    assert polls[0].resolved_url == "https://example.com/feed.xml"
    # The feed keeps the identity it has in the feed file.
    assert polls[0].target.feed_url == "http://example.com/feed.xml"


def test_a_temporary_redirect_before_a_failure_is_not_remembered() -> None:
    class TemporaryRedirectThenError:
        async def get(self, url: str, timeout: float, headers=None):
            request = httpx.Request("GET", "https://example.com/other.xml")
            response = httpx.Response(500, request=request)
            response.history = [_Redirect(302)]
            response.raise_for_status()

    target = FeedPollTarget(
        feed_url="http://example.com/feed.xml",
        source_label="Feed",
        etag=None,
        last_modified=None,
    )
    searcher = GoogleNewsRssSearcher(http_client=TemporaryRedirectThenError())

    polls = asyncio.run(_collect(searcher.poll_feeds([target])))

    assert polls[0].status == "failed"
    assert polls[0].resolved_url is None


def test_a_connection_failure_carries_no_redirect() -> None:
    class Offline:
        async def get(self, url: str, timeout: float, headers=None):
            raise httpx.ConnectError("All connection attempts failed")

    target = FeedPollTarget(
        feed_url="http://example.com/feed.xml",
        source_label="Feed",
        etag=None,
        last_modified=None,
    )
    searcher = GoogleNewsRssSearcher(http_client=Offline())

    polls = asyncio.run(_collect(searcher.poll_feeds([target])))

    assert polls[0].status == "failed"
    assert polls[0].resolved_url is None


def test_explicit_feed_urls_are_not_logged_as_coming_from_the_feed_file(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Naming the default TSV while running an explicitly passed URL reads as
    # if the argument had been ignored.
    class EmptyClient:
        async def get(self, url: str, timeout: float, headers=None):
            return httpx.Response(
                200,
                text="<rss version='2.0'><channel><title>T</title></channel></rss>",
                request=httpx.Request("GET", url),
            )

    searcher = GoogleNewsRssSearcher(http_client=EmptyClient())
    sources = [RssFeedSource("https://example.com/feed.xml", "Example")]

    async def drain():
        async for _ in searcher.iter_batches(sources=sources):
            pass

    with caplog.at_level("INFO"):
        asyncio.run(drain())

    processing = [r.getMessage() for r in caplog.records if "Processing" in r.getMessage()]
    assert processing
    assert "list_queries_ggnews.tsv" not in processing[0]
    assert "1 explicit feed URL(s)" in processing[0]


def test_default_client_follows_redirects() -> None:
    client = GoogleNewsRssSearcher()._build_default_client()
    assert client.follow_redirects is True


def test_default_client_does_not_impersonate_another_crawler() -> None:
    # A Googlebot-shaped User-Agent from a non-Google IP is what bot filters
    # answer with 403, which then counts towards disabling a working feed.
    agent = GoogleNewsRssSearcher()._build_default_client().headers["User-Agent"]

    assert agent == DEFAULT_USER_AGENT
    assert "google" not in agent.lower()


def test_the_user_agent_can_be_overridden() -> None:
    searcher = GoogleNewsRssSearcher(user_agent="acme-reader/2.0 (+acme.test)")

    headers = searcher._build_default_client().headers
    assert headers["User-Agent"] == "acme-reader/2.0 (+acme.test)"


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
        url = "https://feeds.example/0.xml"
        history: list = []

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


# ─── Feed-supplied article content ────────────────────────────────────────────


class _RecordingProvider:
    """Crawl provider that records whether it was asked to fetch anything."""

    def __init__(self) -> None:
        self.crawled: list[str] = []

    async def crawl_batch(self, urls, *, max_concurrency=None):
        self.crawled.extend(urls)
        return [
            CrawlResult(url=url, markdown="# Crawled article body")
            for url in urls
        ]


def _entry_with_content(html: str) -> DiscoveredEntry:
    return DiscoveredEntry(
        feed_url="https://blog.example.com/feed.xml",
        query_label="Example",
        feed_title="Example Blog",
        entry_url="https://blog.example.com/post",
        title="A post",
        source="Example Blog",
        published_at=None,
        content_html=html,
    )


def _searcher_with(provider, **kwargs) -> GoogleNewsRssSearcher:
    return GoogleNewsRssSearcher(
        feed_path="unused.tsv", crawl_provider=provider, **kwargs
    )


def test_full_feed_content_is_stored_without_crawling_the_page() -> None:
    provider = _RecordingProvider()
    searcher = _searcher_with(provider)
    body = "<p>" + ("Real article prose. " * 200) + "</p>"

    batch = asyncio.run(searcher.crawl_entries([_entry_with_content(body)]))

    assert provider.crawled == []
    assert batch.results[0].success is True
    assert "Real article prose." in batch.results[0].markdown
    assert batch.results[0].url == "https://blog.example.com/post"
    assert batch.articles_from_feed_content == 1


def test_crawled_results_and_failures_carry_their_feed_url() -> None:
    class _FailingProvider(_RecordingProvider):
        async def crawl_batch(self, urls, *, max_concurrency=None):
            return [
                CrawlResult(url=url, markdown="", success=False, error_message="boom")
                for url in urls
            ]

    entry = _entry_with_content("")
    ok = asyncio.run(_searcher_with(_RecordingProvider()).crawl_entries([entry]))
    failed = asyncio.run(_searcher_with(_FailingProvider()).crawl_entries([entry]))

    assert ok.results[0].context["feed_url"] == entry.feed_url
    assert failed.results[0].context["feed_url"] == entry.feed_url
    assert failed.failures[0].feed_url == entry.feed_url


def test_a_teaser_still_falls_back_to_crawling() -> None:
    provider = _RecordingProvider()
    searcher = _searcher_with(provider)

    batch = asyncio.run(
        searcher.crawl_entries(
            [_entry_with_content("<p>Short teaser. Read more on our site.</p>")]
        )
    )

    assert provider.crawled == ["https://blog.example.com/post"]
    assert batch.results[0].markdown == "# Crawled article body"
    assert batch.articles_from_feed_content == 0


def test_an_entry_without_feed_content_is_crawled() -> None:
    provider = _RecordingProvider()
    searcher = _searcher_with(provider)

    batch = asyncio.run(searcher.crawl_entries([_entry_with_content("")]))

    assert provider.crawled == ["https://blog.example.com/post"]
    assert batch.articles_from_feed_content == 0


def test_feed_content_substitution_can_be_switched_off() -> None:
    provider = _RecordingProvider()
    searcher = _searcher_with(provider, feed_content_min_chars=0)
    body = "<p>" + ("Real article prose. " * 200) + "</p>"

    batch = asyncio.run(searcher.crawl_entries([_entry_with_content(body)]))

    assert provider.crawled == ["https://blog.example.com/post"]
    assert batch.articles_from_feed_content == 0


def test_the_feed_content_threshold_is_honoured() -> None:
    provider = _RecordingProvider()
    searcher = _searcher_with(provider, feed_content_min_chars=10)

    batch = asyncio.run(
        searcher.crawl_entries([_entry_with_content("<p>Just long enough.</p>")])
    )

    assert provider.crawled == []
    assert batch.articles_from_feed_content == 1


def test_a_teasing_summary_never_becomes_content() -> None:
    # A dek must not replace an article with its own blurb -- not even when the
    # dek happens to be the longer string of the two.
    parsed = _parse_rss_entry(
        {
            "link": "https://blog.example.com/post",
            "title": "A post",
            "summary": "<p>Teaser only.</p>",
            "content": [{"type": "text/html", "value": "<p>Full body.</p>"}],
        },
        "https://blog.example.com/feed.xml",
    )

    assert parsed["content_html"] == "<p>Full body.</p>"

    summary_only = _parse_rss_entry(
        {
            "link": "https://blog.example.com/post",
            "title": "A post",
            "summary": "<p>Teaser only.</p>",
        },
        "https://blog.example.com/feed.xml",
    )

    assert summary_only["content_html"] == ""


def test_a_summary_carrying_the_whole_article_becomes_content() -> None:
    # TechCrunch, Slashdot, Martin Fowler and Etsy all leave <content> empty and
    # put the article in <description>. Ignoring it re-fetched a page the feed
    # had already handed over.
    body = "<p>" + ("The whole article body. " * 80) + "</p>"
    assert len(body) >= 1200

    parsed = _parse_rss_entry(
        {
            "link": "https://blog.example.com/post",
            "title": "A post",
            "summary": body,
        },
        "https://blog.example.com/feed.xml",
    )

    assert parsed["content_html"] == body


def test_the_summary_is_always_captured_separately_from_content() -> None:
    # Whichever tag wins, the dek is stored in its own right: it is the preview
    # text, and it is unrecoverable once the entry drops out of the feed.
    parsed = _parse_rss_entry(
        {
            "link": "https://blog.example.com/post",
            "title": "A post",
            "summary": "<p>Teaser only.</p>",
            "content": [{"type": "text/html", "value": "<p>Full body.</p>"}],
        },
        "https://blog.example.com/feed.xml",
    )

    assert parsed["content_html"] == "<p>Full body.</p>"
    assert parsed["summary_html"] == "<p>Teaser only.</p>"


def _enrich(result: CrawlResult, *, entry_title: str = "The real headline"):
    searcher = GoogleNewsRssSearcher()
    job = _ArticleJob(
        {
            "feed_entry_url": "https://www.nytimes.com/2026/08/22/technology/pigeons.html",
            "title": entry_title,
            "source": "NYT",
            "published_date": None,
            "summary_html": "<p>A dek long enough to clear the summary threshold "
            "and stand on its own as a preview.</p>",
        },
        "Technology",
        "NYT > Technology",
    )
    return searcher._enrich_result(result, job, result.url)


@pytest.mark.parametrize(
    "block_page_title",
    [
        # Every one of these was stored as an article headline before the fix.
        "nytimes.com",                                 # DataDome
        "ERROR: The request could not be satisfied",   # CloudFront
        "Human Verification",                          # Imperva
    ],
)
def test_a_blocked_crawl_does_not_donate_its_title_to_the_article(
    block_page_title: str,
) -> None:
    blocked = CrawlResult(
        url="https://www.nytimes.com/2026/08/22/technology/pigeons.html",
        markdown="",
        success=False,
        error_message="Blocked by anti-bot protection: DataDome captcha",
        metadata=CrawlMetadata(title=block_page_title, language="en"),
    )

    enriched = _enrich(blocked)

    assert enriched.metadata.title == "The real headline"
    # The interstitial's language is no more trustworthy than its title.
    assert enriched.metadata.language is None


def test_a_successful_crawl_still_prefers_its_own_title() -> None:
    crawled = CrawlResult(
        url="https://www.nytimes.com/2026/08/22/technology/pigeons.html",
        markdown="# The article\n\nBody.",
        metadata=CrawlMetadata(title="The page's fuller headline", language="en"),
    )

    enriched = _enrich(crawled)

    assert enriched.metadata.title == "The page's fuller headline"
    assert enriched.metadata.language == "en"


def test_the_longest_content_block_wins() -> None:
    parsed = _parse_rss_entry(
        {
            "link": "https://blog.example.com/post",
            "title": "A post",
            "content": [
                {"type": "text/html", "value": "<p>Short.</p>"},
                {"type": "text/html", "value": "<p>A considerably longer body.</p>"},
            ],
        },
        "https://blog.example.com/feed.xml",
    )

    assert parsed["content_html"] == "<p>A considerably longer body.</p>"
