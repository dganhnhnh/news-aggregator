from datetime import timezone

from adapters.retrieve_news_data.helpers import extract_date_from_result


def test_extract_date_from_url_when_provider_has_no_date() -> None:
    result = extract_date_from_result(
        published_date=None,
        url="https://example.com/2026/08/05/article",
    )

    assert result is not None
    assert result.isoformat() == "2026-08-05T00:00:00+00:00"
    assert result.tzinfo == timezone.utc
