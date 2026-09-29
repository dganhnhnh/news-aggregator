from core.boilerplate import (
    MIN_DOCS_PER_SOURCE,
    build_profile,
    is_navline,
    normalize_line,
    strip_boilerplate,
    visible_text,
)


# The shape that made every WIRED article embed to nearly the same vector: a
# consent panel long enough to fill the embedder's 1200-character window, in
# front of an article that varies.
CONSENT_PANEL = (
    "We and our partners use cookies and similar technologies to store and "
    "access information on a device.\n"
    "Some partners process personal data on the basis of legitimate interest, "
    "which you may object to at any time.\n"
)


def _article(body: str) -> str:
    return CONSENT_PANEL + body


def test_visible_text_drops_link_targets_and_markdown_syntax() -> None:
    assert visible_text("* **[Politics](https://example.com/politics)**") == "Politics"


def test_normalize_line_collapses_one_chrome_line_written_three_ways() -> None:
    keys = {
        normalize_line("  * Notifications"),
        normalize_line("Notifications"),
        normalize_line("[Notifications](/account/alerts)"),
    }
    assert keys == {"notifications"}


def test_is_navline_separates_a_menu_row_from_a_sentence_with_links() -> None:
    menu = "[News](/n) [Video](/v) [Reviews](/r) [Deals](/d)"
    sentence = (
        "The [company](https://example.com/c) told [regulators](https://example.com/r) "
        "that its [filing](https://example.com/f) was complete."
    )
    assert is_navline(menu)
    assert not is_navline(sentence)


def test_build_profile_needs_a_corpus_before_it_calls_anything_chrome() -> None:
    contents = [_article(f"Story {i} body.") for i in range(MIN_DOCS_PER_SOURCE - 1)]
    assert build_profile(contents) == frozenset()


def test_build_profile_catches_a_panel_that_repeats_across_the_source() -> None:
    profile = build_profile([_article(f"Story {i} body.") for i in range(10)])
    assert normalize_line(CONSENT_PANEL.splitlines()[0]) in profile
    assert normalize_line("Story 3 body.") not in profile


def test_build_profile_catches_a_menu_row_that_repeats_only_twice() -> None:
    """A site that varies its nav per section never gets one variant over the
    document-frequency bar, which is why repeated menu rows qualify on sight."""
    menu = "[Security](/sec) [Business](/biz) [Culture](/cul) [Science](/sci)"
    contents = [f"{menu}\nStory {i} body." for i in range(2)] + [
        f"Story {i} body." for i in range(2, 10)
    ]
    assert normalize_line(menu) in build_profile(contents)


def test_strip_boilerplate_leaves_content_alone_without_a_profile() -> None:
    content = _article("Story body.")
    assert strip_boilerplate(content, frozenset()) == content


def test_strip_boilerplate_removes_chrome_interleaved_with_the_article() -> None:
    """The Verge puts "Loading comments" *between* the headline and the body,
    so cutting a single contiguous leading block would leave chrome behind."""
    contents = [
        f"Notifications\nA headline long enough to be real article text {i}.\n"
        f"Loading comments\nThe body of story {i} runs on for a while here."
        for i in range(10)
    ]
    profile = build_profile(contents)
    stripped = strip_boilerplate(contents[0], profile)
    assert "Notifications" not in stripped
    assert "Loading comments" not in stripped
    assert "A headline long enough to be real article text 0." in stripped
    assert "The body of story 0" in stripped


def test_strip_boilerplate_keeps_a_recurring_template_lead_paragraph() -> None:
    """Mashable opens every deal post with "**TL;DR:**" over a paragraph that
    is genuinely this article's text. Cutting everything above the last chrome
    line would delete it."""
    contents = [
        f"[Deals](/deals) [Tech](/tech) [Science](/sci) [Culture](/c)\n"
        f"**TL;DR:** As of September {i}, the widget is down to ${i}9 at Example."
        for i in range(1, 11)
    ]
    stripped = strip_boilerplate(contents[0], build_profile(contents))
    assert "the widget is down to $19 at Example" in stripped
    assert "[Deals](/deals)" not in stripped


def test_strip_boilerplate_never_removes_a_line_outside_the_profile() -> None:
    profile = frozenset({"notifications"})
    content = "Notifications\nAn article about cookie consent banners in the EU."
    assert strip_boilerplate(content, profile) == (
        "An article about cookie consent banners in the EU."
    )
