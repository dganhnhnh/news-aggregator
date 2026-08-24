from core.html_content import (
    MIN_FULL_CONTENT_CHARS,
    html_to_markdown,
    is_full_article,
)


def test_article_html_becomes_markdown() -> None:
    markdown = html_to_markdown(
        "<h1>Title</h1><p>First <strong>paragraph</strong>.</p>"
        '<p>See <a href="https://example.com/x">this</a>.</p>'
    )

    assert "# Title" in markdown
    assert "**paragraph**" in markdown
    assert "https://example.com/x" in markdown


def test_images_are_dropped_but_their_captions_survive() -> None:
    markdown = html_to_markdown(
        '<p><img src="https://example.com/a.png" alt="chart"></p><p>Body text.</p>'
    )

    assert "a.png" not in markdown
    assert "Body text." in markdown


def test_paragraphs_are_not_hard_wrapped() -> None:
    # Hard wrapping would make stored articles diff noisily against crawled
    # ones, which are not wrapped either.
    sentence = "word " * 60
    markdown = html_to_markdown(f"<p>{sentence}</p>")

    assert len(markdown.splitlines()) == 1


def test_empty_and_malformed_html_render_to_nothing_rather_than_raising() -> None:
    assert html_to_markdown("") == ""
    assert html_to_markdown("   ") == ""
    # A feed body is untrusted input; it must never take a run down.
    assert isinstance(html_to_markdown("<p>unclosed <div><span>"), str)


def test_a_teaser_is_not_accepted_as_a_full_article() -> None:
    teaser = html_to_markdown("<p>A short summary. Read more at our site.</p>")

    assert is_full_article(teaser) is False


def test_a_long_body_is_accepted_as_a_full_article() -> None:
    body = html_to_markdown("<p>" + ("Real article text. " * 200) + "</p>")

    assert len(body) >= MIN_FULL_CONTENT_CHARS
    assert is_full_article(body) is True


def test_the_threshold_is_adjustable() -> None:
    short = html_to_markdown("<p>Tiny body.</p>")

    assert is_full_article(short, min_chars=5) is True
    assert is_full_article(short, min_chars=10_000) is False
