from services.text import embed_text, first_paragraphs


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


def test_embed_text_joins_title_and_body() -> None:
    result = embed_text("Headline", "Body paragraph.")
    assert result == "Headline\n\nBody paragraph."


def test_embed_text_without_title_is_just_the_body() -> None:
    result = embed_text(None, "Body paragraph.")
    assert result == "Body paragraph."
