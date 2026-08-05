from pathlib import Path

import pytest

from adapters.retrieve_news_data.google_news_rss_searcher import GoogleNewsRssSearcher


def test_load_queries_from_tsv(tmp_path: Path) -> None:
    path = tmp_path / "queries.tsv"
    path.write_text(
        "tier\tgroup\tGold market\thttps://news.google.com/rss/search?q=gold\n",
        encoding="utf-8",
    )

    assert GoogleNewsRssSearcher._load_queries_from_tsv(path) == [
        ("Gold market", "https://news.google.com/rss/search?q=gold")
    ]


def test_missing_tsv_has_actionable_error(tmp_path: Path) -> None:
    missing = tmp_path / "missing.tsv"
    searcher = GoogleNewsRssSearcher(tsv_path=missing)

    with pytest.raises(FileNotFoundError, match="GOOGLE_NEWS_TSV_PATH"):
        searcher._load_queries()
