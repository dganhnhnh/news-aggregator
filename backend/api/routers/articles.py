"""Read-only endpoints over the `raw_articles` table."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from adapters.storage import PostgresRawStorage
from api.deps import get_storage
from api.schemas import ArticleDetail, ArticleSummary, Page


router = APIRouter(prefix="/api/articles", tags=["articles"])


@router.get("", response_model=Page[ArticleSummary])
async def list_articles(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    source: str | None = None,
    q: str | None = None,
    storage: PostgresRawStorage = Depends(get_storage),
) -> Page[ArticleSummary]:
    items, total = await storage.list_articles(
        limit=limit, offset=offset, source=source, query=q
    )
    return Page(
        items=[ArticleSummary(**item.__dict__) for item in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{article_id}", response_model=ArticleDetail)
async def get_article(
    article_id: int,
    storage: PostgresRawStorage = Depends(get_storage),
) -> ArticleDetail:
    article = await storage.get_article(article_id)
    if article is None:
        raise HTTPException(status_code=404, detail="Article not found")
    return ArticleDetail(
        id=article_id,
        url=article.url,
        title=article.title,
        source=article.source,
        content=article.content,
        published_at=article.published_at,
        crawled_at=article.crawled_at,
    )
