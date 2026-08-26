from core.content_quality import (
    BLOCK_PAGE_CONTEXT_KEY,
    block_page_reason,
    crawl_failure_error_type,
    historical_summary_title_reason,
    is_nav_dense,
)
from core.urls import URL_ERROR_CONTEXT_KEY


# The shape of the Imperva interstitial that was stored as an article fourteen
# times: HTTP 200, a real title, a paragraph of prose, ~950 characters.
IMPERVA_PAGE = """# Human Verification

Please complete the security check to access this site.

Additional security check is required. Please confirm you are human by
completing the action below.

Reference ID: 0a3f19c4-77b2-4e11-9c0a-5f2d8b1e6a90
"""


def test_imperva_human_verification_page_is_rejected() -> None:
    assert block_page_reason("Human Verification", IMPERVA_PAGE)


def test_block_title_is_matched_regardless_of_case_and_punctuation() -> None:
    for title in ("Just a moment...", "JUST A MOMENT", "Just a moment…"):
        assert block_page_reason(title, "# Body"), title


def test_cloudfront_request_block_page_is_rejected_by_title() -> None:
    assert block_page_reason("ERROR: The request could not be satisfied", "")


def test_historical_summary_cleanup_recognizes_publisher_host_title() -> None:
    assert historical_summary_title_reason(
        "nytimes.com",
        "https://www.nytimes.com/2026/08/technology/example.html",
    )


def test_historical_summary_cleanup_keeps_a_real_feed_title() -> None:
    assert (
        historical_summary_title_reason(
            "Open-source model release changes AI tooling",
            "https://www.nytimes.com/2026/08/technology/example.html",
        )
        == ""
    )


def test_interstitial_phrase_near_the_top_of_a_short_page_is_rejected() -> None:
    page = "Checking your browser before accessing example.com.\n\nRay ID: 8f2a"

    reason = block_page_reason("example.com", page)

    assert "checking your browser before accessing" in reason


def test_long_article_about_captchas_is_kept() -> None:
    """The rule that matters most: a real article may discuss block pages.

    Ars, Slashdot and The Register all publish these. Rejecting them would be
    a silent, permanent hole in coverage of exactly one topic.
    """
    article = (
        "# Cloudflare outage took down a third of the web\n\n"
        + "Analysis of the incident follows. " * 40
        + "\n\nUsers instead saw a page reading 'Checking your browser before "
        "accessing', with a Ray ID: 8f2a1c and no way to continue. "
        + "The company said the rollout was to blame. " * 40
    )
    assert len(article) > 2500

    assert block_page_reason("Cloudflare outage took down a third of the web", article) == ""


def test_short_article_whose_phrase_appears_late_is_kept() -> None:
    page = (
        "# Weekly security roundup\n\n"
        + "Nothing unusual happened this week in the sector. " * 14
        + "\n\nOne reader reported a 'confirm you are human' prompt."
    )

    assert block_page_reason("Weekly security roundup", page) == ""


def test_ordinary_article_is_kept() -> None:
    assert block_page_reason("Nvidia beats earnings estimates", "# Nvidia\n\nThe chipmaker...") == ""


def test_missing_title_falls_through_to_the_phrase_rule() -> None:
    assert block_page_reason(None, "# Article\n\nBody text.") == ""
    assert block_page_reason(None, "Please verify you are a human to continue.")


def test_a_masthead_and_category_bar_read_as_nav_dense() -> None:
    chrome = "".join(f"* [Section {i}](https://example.com/s{i})\n" for i in range(30))

    assert is_nav_dense(chrome)


def test_an_article_body_with_a_few_citations_is_not_nav_dense() -> None:
    body = (
        "The company said the rollout was to blame. "
        "[Its postmortem](https://example.com/a) describes a config push, and "
        "[an earlier incident](https://example.com/b) had the same cause. "
    ) * 3

    assert not is_nav_dense(body)


def test_links_past_the_opening_window_do_not_count() -> None:
    """Nothing embeds past ~1200 characters, so a footer link farm is harmless."""
    article = "Real article text. " * 80
    assert len(article) > 1200
    footer = "".join(f"[Link {i}](https://example.com/{i})\n" for i in range(40))

    assert not is_nav_dense(article + footer)


def test_error_type_prefers_unusable_url_over_block_page() -> None:
    context = {
        URL_ERROR_CONTEXT_KEY: "relative_url",
        BLOCK_PAGE_CONTEXT_KEY: "block-page title: 'access denied'",
    }

    assert crawl_failure_error_type(context) == "UnusableUrl:relative_url"


def test_error_type_classifies_block_pages_and_defaults_to_crawl_error() -> None:
    assert (
        crawl_failure_error_type({BLOCK_PAGE_CONTEXT_KEY: "block-page title: 'x'"})
        == "BlockedPage"
    )
    assert crawl_failure_error_type({}) == "CrawlError"
