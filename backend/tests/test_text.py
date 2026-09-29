from core.boilerplate import normalize_line
from services.text import article_embed_text, article_text, first_paragraphs


# Roughly what CNET and Slashdot crawl into: a masthead and a category bar,
# with the article somewhere past the embedder's 1200-character horizon.
NAV_CHROME = "".join(f"* [Section {i}](https://example.com/s{i})\n" for i in range(30))
DEK = (
    "Hugging Face is using the breach to push for openness in AI development, "
    "arguing that closed models make incidents like this harder to audit."
)


def test_first_paragraphs_strips_code_fences() -> None:
    content = "Intro paragraph.\n\n```python\nprint('x' * 5000)\n```\n\nOutro paragraph."
    result = first_paragraphs(content, max_chars=1200)
    assert "print" not in result
    assert "Intro paragraph." in result
    assert "Outro paragraph." in result


def test_first_paragraphs_drops_bare_link_lines() -> None:
    content = "Real sentence.\n\n[Read more](https://example.com/x)\n\nAnother sentence."
    result = first_paragraphs(content, max_chars=1200)
    assert "https://example.com/x" not in result
    assert "Real sentence." in result
    assert "Another sentence." in result


def test_first_paragraphs_keeps_short_content_untouched() -> None:
    content = "Short article body."
    assert first_paragraphs(content, max_chars=1200) == content


def test_first_paragraphs_truncates_on_a_word_boundary() -> None:
    content = " ".join(["word"] * 500)
    result = first_paragraphs(content, max_chars=50)
    assert len(result) <= 50
    assert not result.endswith("wor")
    assert result == content[: len(result)]


def test_a_clean_crawled_body_is_used() -> None:
    body, kind = article_text(
        content="The chipmaker reported record quarterly revenue.",
        feed_summary=DEK,
        content_source="crawl",
    )

    assert kind == "content"
    assert body.startswith("The chipmaker")


def test_a_feed_body_is_used_even_when_it_looks_link_heavy() -> None:
    """A feed body is the publisher's own article HTML, so its links are the
    article's links -- there is no site chrome to mistake them for."""
    body, kind = article_text(
        content=NAV_CHROME + "\n\nThe article itself.",
        feed_summary=DEK,
        content_source="feed_content",
    )

    assert kind == "content"
    assert "Section 0" in body


def test_a_nav_dense_crawl_loses_to_the_feed_dek() -> None:
    body, kind = article_text(
        content=NAV_CHROME + "\n\nThe article itself.",
        feed_summary=DEK,
        content_source="crawl",
    )

    assert kind == "feed_summary"
    assert body == DEK


def test_a_nav_dense_crawl_without_a_dek_falls_back_to_title_only() -> None:
    """Publisher chrome must not become the semantic representation."""
    content = NAV_CHROME + "\n\nThe article itself."

    body, kind = article_text(content=content, feed_summary="", content_source="crawl")

    assert kind == "title"
    assert body == ""


def test_a_dek_too_short_to_say_anything_falls_back_to_title_only() -> None:
    body, kind = article_text(
        content=NAV_CHROME, feed_summary="Comments", content_source="crawl"
    )

    assert kind == "title"
    assert body == ""


def test_embed_text_uses_the_title_when_nav_dense_content_has_no_dek() -> None:
    text, kind = article_embed_text(
        title="A distinct article headline",
        content=NAV_CHROME + "\n\nThe article itself.",
        feed_summary="",
        content_source="crawl",
    )

    assert kind == "title"
    assert text == "A distinct article headline"


def test_an_article_with_no_text_at_all_falls_back_to_the_title() -> None:
    assert article_text(content="", feed_summary="", content_source="feed_summary") == (
        "",
        "title",
    )


def test_a_summary_only_article_embeds_its_dek_under_its_title() -> None:
    text, kind = article_embed_text(
        title="Hugging Face breached by rogue bots",
        content="",
        feed_summary=DEK,
        content_source="feed_summary",
    )

    assert kind == "feed_summary"
    assert text == f"Hugging Face breached by rogue bots\n\n{DEK}"


def test_an_untitled_article_embeds_just_its_body() -> None:
    text, kind = article_embed_text(
        title=None,
        content="The chipmaker reported record quarterly revenue.",
        feed_summary="",
        content_source="crawl",
    )

    assert kind == "content"
    assert text == "The chipmaker reported record quarterly revenue."


def test_a_title_only_article_embeds_just_the_title() -> None:
    text, kind = article_embed_text(
        title="A headline",
        content="",
        feed_summary="",
        content_source="feed_summary",
    )

    assert kind == "title"
    assert text == "A headline"


# A consent panel is prose, so `is_nav_dense` sees an ordinary article and the
# crawled body wins rung 2 -- with the banner, not the article, inside the
# embedder's window. This is what one source's profile is for.
CONSENT_PANEL = (
    "We and our partners use cookies and similar technologies to store and "
    "access information on a device, such as unique IDs in cookies.\n"
    "Some partners process personal data on the basis of legitimate interest, "
    "which you may object to at any time via the preferences link.\n"
)
CONSENT_PROFILE = frozenset(
    normalize_line(line) for line in CONSENT_PANEL.splitlines()
)


def test_a_consent_panel_survives_nav_density_and_wins_without_a_profile() -> None:
    body, kind = article_text(
        content=CONSENT_PANEL + "Stoke Space raised another round.",
        feed_summary=DEK,
        content_source="crawl",
    )

    assert kind == "content"
    assert body.startswith("We and our partners")


def test_the_source_profile_puts_the_article_back_at_the_front() -> None:
    body, kind = article_text(
        content=CONSENT_PANEL + "Stoke Space raised another round of funding.",
        feed_summary=DEK,
        content_source="crawl",
        boilerplate=CONSENT_PROFILE,
    )

    assert kind == "content"
    assert body == "Stoke Space raised another round of funding."


def test_a_feed_body_is_stripped_too() -> None:
    """Rung 1 trusts publisher HTML, but a line the profile names is one the
    publisher repeats on every article either way."""
    body, kind = article_text(
        content=CONSENT_PANEL + "The article itself, as the feed shipped it.",
        feed_summary=DEK,
        content_source="feed_content",
        boilerplate=CONSENT_PROFILE,
    )

    assert kind == "content"
    assert body == "The article itself, as the feed shipped it."


def test_stripping_a_page_down_to_nothing_falls_back_to_the_dek() -> None:
    body, kind = article_text(
        content=CONSENT_PANEL,
        feed_summary=DEK,
        content_source="crawl",
        boilerplate=CONSENT_PROFILE,
    )

    assert kind == "feed_summary"
    assert body == DEK


def test_two_articles_behind_one_banner_stop_embedding_to_the_same_text() -> None:
    first, _ = article_embed_text(
        title="Crunch time for humanity",
        content=CONSENT_PANEL + "An AI researcher quit Anthropic this week.",
        feed_summary="",
        content_source="crawl",
        boilerplate=CONSENT_PROFILE,
    )
    second, _ = article_embed_text(
        title="Stoke Space goes even bigger",
        content=CONSENT_PANEL + "The launch startup raised another round.",
        feed_summary="",
        content_source="crawl",
        boilerplate=CONSENT_PROFILE,
    )

    assert "cookies" not in first and "cookies" not in second
    assert first != second
