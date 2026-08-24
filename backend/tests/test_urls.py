import pytest

from core.urls import (
    SCHEME_RELATIVE_BASE,
    UnusableUrl,
    is_probably_article,
    resolve_crawl_url,
)


@pytest.mark.parametrize(
    "label, href, base, expected",
    [
        (
            "root relative",
            "/2020/06/22/post.html",
            "https://blog.example.com/feed.xml",
            "https://blog.example.com/2020/06/22/post.html",
        ),
        (
            # A base path ending in "/" is a directory, so the href hangs off
            # it rather than replacing its last segment.
            "path relative",
            "2018/some-post/",
            "https://tech.finn.no/feed/",
            "https://tech.finn.no/feed/2018/some-post/",
        ),
        (
            "path relative to a file base",
            "2018/some-post/",
            "https://tech.finn.no/feed.xml",
            "https://tech.finn.no/2018/some-post/",
        ),
        (
            "protocol relative",
            "//cdn.example.com/post",
            "https://example.com/feed.xml",
            "https://cdn.example.com/post",
        ),
        (
            "already absolute",
            "https://other.example.com/post",
            "https://example.com/feed.xml",
            "https://other.example.com/post",
        ),
    ],
)
def test_relative_links_resolve_against_their_base(
    label: str, href: str, base: str, expected: str
) -> None:
    assert resolve_crawl_url(href, base_url=base) == expected


def test_protocol_relative_cli_input_defaults_to_https() -> None:
    # The case that motivated this: `news-pipeline crawl //host/path` has no
    # containing document, so the scheme has to come from somewhere.
    resolved = resolve_crawl_url(
        "//target.github.io/architecture/modernization_Intro",
        base_url=SCHEME_RELATIVE_BASE,
    )

    assert resolved == "https://target.github.io/architecture/modernization_Intro"


@pytest.mark.parametrize(
    "href, reason",
    [
        ("", "empty"),
        ("   ", "empty"),
        ("/only/a/path", "relative"),
        ("example.com/no-scheme", "relative"),
        ("ftp://example.com/file", "unsupported_scheme"),
        ("mailto:editor@example.com", "unsupported_scheme"),
        ("javascript:void(0)", "unsupported_scheme"),
    ],
)
def test_unresolvable_urls_are_rejected_with_a_stable_reason(
    href: str, reason: str
) -> None:
    with pytest.raises(UnusableUrl) as excinfo:
        resolve_crawl_url(href)

    assert excinfo.value.reason == reason


def test_a_base_url_does_not_rescue_an_unsupported_scheme() -> None:
    with pytest.raises(UnusableUrl) as excinfo:
        resolve_crawl_url("mailto:editor@example.com", base_url="https://example.com/")

    assert excinfo.value.reason == "unsupported_scheme"


@pytest.mark.parametrize(
    "href, link_type, expected",
    [
        ("https://example.com/post", "", True),
        ("https://example.com/post.html", "text/html", True),
        ("https://example.com/episode.mp3", "", False),
        ("https://example.com/audio", "audio/mpeg", False),
        ("https://example.com/audio", "audio/mpeg; charset=utf-8", False),
        ("https://example.com/clip", "video/mp4", False),
        ("https://example.com/doc", "application/pdf", False),
        ("https://example.com/paper.pdf", "", False),
        # A query string must not disguise a media file, nor create a false
        # positive on a normal article URL.
        ("https://example.com/episode.mp3?utm_source=rss", "", False),
        ("https://example.com/post?file=x.mp3", "", True),
    ],
)
def test_media_links_are_not_treated_as_articles(
    href: str, link_type: str, expected: bool
) -> None:
    assert is_probably_article(href, link_type) is expected
