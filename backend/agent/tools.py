"""Read-only LangChain tools backed by News Radar's PostgreSQL repository."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from langchain.messages import ToolMessage
from langchain.tools import ToolRuntime, tool
from langgraph.types import Command
from pydantic import BaseModel, Field, field_validator, model_validator

from agent.state import Evidence, ResearchContext
from core.logger import get_logger


logger = get_logger(__name__)


class TimeRange(BaseModel):
    since: datetime | None = None
    until: datetime | None = None

    @field_validator("since", "until")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and (
            value.tzinfo is None or value.utcoffset() is None
        ):
            raise ValueError("timestamps must include a timezone")
        return value.astimezone(timezone.utc) if value is not None else None

    @model_validator(mode="after")
    def validate_order(self) -> "TimeRange":
        if self.since is not None and self.until is not None and self.since >= self.until:
            raise ValueError("since must be earlier than until")
        return self


class SearchArticlesInput(BaseModel):
    query: str = Field(min_length=1, max_length=4000)
    time_range: TimeRange | None = None
    source: str | None = Field(default=None, max_length=200)

    @field_validator("query", "source", mode="before")
    @classmethod
    def trim_text(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class SearchStorylinesInput(BaseModel):
    query: str = Field(min_length=1, max_length=4000)
    time_range: TimeRange | None = None

    @field_validator("query", mode="before")
    @classmethod
    def trim_query(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class ArticleIdInput(BaseModel):
    article_id: int = Field(gt=0)


class StorylineIdInput(BaseModel):
    storyline_id: int = Field(gt=0)


def _source(slug: str | None, name: str | None) -> dict[str, str] | None:
    if slug is None:
        return None
    return {"slug": slug, "name": name or slug}


def _article_evidence(
    *,
    article_id: int,
    url: str,
    title: str | None,
    source_slug: str | None,
    source_name: str | None,
    published_at: datetime | None,
    event_at: datetime,
    snippet: str | None = None,
    content: str | None = None,
    feed_summary: str | None = None,
    topics: list[dict[str, Any]] | None = None,
) -> Evidence:
    evidence: Evidence = {
        "kind": "article",
        "article_id": article_id,
        "citation": f"[A{article_id}]",
        "url": url,
        "title": title,
        "source": _source(source_slug, source_name),
        "published_at": published_at,
        "event_at": event_at,
    }
    if snippet is not None:
        evidence["snippet"] = snippet
    if content is not None:
        evidence["content"] = content
    if feed_summary is not None:
        evidence["feed_summary"] = feed_summary
    if topics is not None:
        evidence["topics"] = topics
    return evidence


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _command(runtime: ToolRuntime, payload: dict[str, Any], evidence: list[Evidence]) -> Command:
    message = ToolMessage(
        content=json.dumps(payload, ensure_ascii=False, default=_json_default),
        tool_call_id=runtime.tool_call_id,
    )
    # Publish each result immediately, even while sibling tools are still running.
    runtime.stream_writer({"type": "tool_result", "message": message})
    return Command(
        update={
            "evidence": evidence,
            "messages": [message],
        }
    )


def _bounds(time_range: TimeRange | None) -> tuple[datetime | None, datetime | None]:
    if time_range is None:
        return None, None
    return time_range.since, time_range.until


def _log_call(context: ResearchContext, tool_name: str, **fields: Any) -> None:
    logger.info(
        "Research tool call request_id=%s tool=%s input=%s",
        context.request_id,
        tool_name,
        json.dumps(fields, ensure_ascii=False, default=_json_default),
    )


def _log_result(context: ResearchContext, tool_name: str, **fields: Any) -> None:
    logger.info(
        "Research tool result request_id=%s tool=%s result=%s",
        context.request_id,
        tool_name,
        json.dumps(fields, ensure_ascii=False, default=_json_default),
    )


@tool(args_schema=SearchArticlesInput)
async def search_articles(
    query: str,
    runtime: ToolRuntime,
    time_range: TimeRange | None = None,
    source: str | None = None,
) -> Command:
    """Hybrid-search articles by meaning and full text.

    Use an optional timezone-aware time range for date-sensitive questions and
    an exact source slug or source name when the user names a publisher.
    """
    context: ResearchContext = runtime.context
    clean_query = query.strip()
    _log_call(
        context,
        "search_articles",
        query=clean_query,
        time_range=time_range.model_dump() if time_range else None,
        source=source,
    )
    vector = await context.embedder.embed_query(clean_query)
    since, until = _bounds(time_range)
    hits = await context.storage.search_articles(
        clean_query,
        vector,
        model=context.embedder.model_name,
        since=since,
        until=until,
        source=source.strip() if source else None,
    )
    items = [
        {
            "id": hit.id,
            "citation": f"[A{hit.id}]",
            "title": hit.title,
            "source": _source(hit.source_slug, hit.source_name),
            "published_at": hit.published_at,
            "event_at": hit.event_at,
            "snippet": hit.snippet,
            "url": hit.url,
        }
        for hit in hits
    ]
    evidence = [
        _article_evidence(
            article_id=hit.id,
            url=hit.url,
            title=hit.title,
            source_slug=hit.source_slug,
            source_name=hit.source_name,
            published_at=hit.published_at,
            event_at=hit.event_at,
            snippet=hit.snippet,
        )
        for hit in hits
    ]
    _log_result(
        context,
        "search_articles",
        count=len(hits),
        article_ids=[hit.id for hit in hits],
    )
    return _command(runtime, {"ok": True, "articles": items}, evidence)


@tool(args_schema=SearchStorylinesInput)
async def search_storylines(
    query: str,
    runtime: ToolRuntime,
    time_range: TimeRange | None = None,
) -> Command:
    """Semantic-search storylines, represented by canonical event clusters.

    Results are navigation evidence. Read member articles before making detailed
    factual claims; storyline IDs are never valid factual citations.
    """
    context: ResearchContext = runtime.context
    clean_query = query.strip()
    _log_call(
        context,
        "search_storylines",
        query=clean_query,
        time_range=time_range.model_dump() if time_range else None,
    )
    vector = await context.embedder.embed_query(clean_query)
    since, until = _bounds(time_range)
    hits = await context.storage.search_storylines(
        vector, since=since, until=until
    )
    items: list[dict[str, Any]] = []
    evidence: list[Evidence] = []
    for hit in hits:
        members = [
            {
                "article_id": member.article_id,
                "citation": f"[A{member.article_id}]",
                "title": member.title,
                "url": member.url,
                "source": _source(member.source_slug, member.source_name),
                "published_at": member.published_at,
                "event_at": member.event_at,
                "snippet": member.snippet,
            }
            for member in hit.member_preview
        ]
        items.append(
            {
                "id": hit.id,
                "title": hit.title,
                "summary": None,
                "updated_at": hit.updated_at,
                "article_count": hit.article_count,
                "member_preview": members,
            }
        )
        evidence.append(
            {
                "kind": "storyline",
                "storyline_id": hit.id,
                "title": hit.title,
                "summary": None,
                "updated_at": hit.updated_at,
                "article_count": hit.article_count,
                "members": members,
            }
        )
        evidence.extend(
            _article_evidence(
                article_id=member.article_id,
                url=member.url,
                title=member.title,
                source_slug=member.source_slug,
                source_name=member.source_name,
                published_at=member.published_at,
                event_at=member.event_at,
                snippet=member.snippet,
            )
            for member in hit.member_preview
        )
    _log_result(
        context,
        "search_storylines",
        count=len(hits),
        storyline_ids=[hit.id for hit in hits],
    )
    return _command(runtime, {"ok": True, "storylines": items}, evidence)


@tool(args_schema=ArticleIdInput)
async def get_article(article_id: int, runtime: ToolRuntime) -> Command:
    """Read one article's full stored content when deeper evidence is needed."""
    context: ResearchContext = runtime.context
    _log_call(context, "get_article", article_id=article_id)
    article = await context.storage.get_article(article_id)
    if article is None:
        _log_result(context, "get_article", article_id=article_id, found=False)
        return _command(
            runtime,
            {"ok": False, "error": "article_not_found", "article_id": article_id},
            [],
        )
    topics = [
        {"id": topic.id, "slug": topic.slug, "name": topic.display_name}
        for topic in article.topics
    ]
    payload = {
        "ok": True,
        "article": {
            "id": article_id,
            "citation": f"[A{article_id}]",
            "url": article.url,
            "url_original": article.url_original,
            "title": article.title,
            "content": article.content,
            "feed_summary": article.feed_summary,
            "source": _source(article.source_slug, article.source_name),
            "published_at": article.published_at,
            "crawled_at": article.crawled_at,
            "event_at": article.event_at,
            "topics": topics,
        },
    }
    evidence = [
        _article_evidence(
            article_id=article_id,
            url=article.url,
            title=article.title,
            source_slug=article.source_slug,
            source_name=article.source_name,
            published_at=article.published_at,
            event_at=article.event_at,
            content=article.content,
            feed_summary=article.feed_summary,
            topics=topics,
        )
    ]
    _log_result(
        context,
        "get_article",
        article_id=article_id,
        found=True,
        content_chars=len(article.content),
        feed_summary_chars=len(article.feed_summary),
    )
    return _command(runtime, payload, evidence)


@tool(args_schema=StorylineIdInput)
async def get_storyline(storyline_id: int, runtime: ToolRuntime) -> Command:
    """Read a canonical storyline and all member article previews."""
    context: ResearchContext = runtime.context
    _log_call(context, "get_storyline", storyline_id=storyline_id)
    storyline = await context.storage.get_storyline_evidence(storyline_id)
    if storyline is None:
        _log_result(
            context, "get_storyline", storyline_id=storyline_id, found=False
        )
        return _command(
            runtime,
            {
                "ok": False,
                "error": "storyline_not_found",
                "storyline_id": storyline_id,
            },
            [],
        )
    members = [
        {
            "article_id": member.article_id,
            "citation": f"[A{member.article_id}]",
            "title": member.title,
            "url": member.url,
            "source": _source(member.source_slug, member.source_name),
            "published_at": member.published_at,
            "event_at": member.event_at,
            "snippet": member.snippet,
        }
        for member in storyline.members
    ]
    payload = {
        "ok": True,
        "storyline": {
            "id": storyline.id,
            "title": storyline.title,
            "summary": None,
            "first_seen_at": storyline.first_seen_at,
            "updated_at": storyline.updated_at,
            "article_count": storyline.article_count,
            "source_count": storyline.source_count,
            "members": members,
        },
    }
    evidence: list[Evidence] = [
        {
            "kind": "storyline",
            "storyline_id": storyline.id,
            "requested_storyline_id": storyline_id,
            "title": storyline.title,
            "summary": None,
            "first_seen_at": storyline.first_seen_at,
            "updated_at": storyline.updated_at,
            "article_count": storyline.article_count,
            "source_count": storyline.source_count,
            "members": members,
        }
    ]
    evidence.extend(
        _article_evidence(
            article_id=member.article_id,
            url=member.url,
            title=member.title,
            source_slug=member.source_slug,
            source_name=member.source_name,
            published_at=member.published_at,
            event_at=member.event_at,
            snippet=member.snippet,
        )
        for member in storyline.members
    )
    _log_result(
        context,
        "get_storyline",
        requested_storyline_id=storyline_id,
        canonical_storyline_id=storyline.id,
        found=True,
        article_count=len(storyline.members),
        article_ids=[member.article_id for member in storyline.members],
    )
    return _command(runtime, payload, evidence)
