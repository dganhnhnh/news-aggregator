"""Stateless natural-language research endpoint."""

from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Response

from agent.service import (
    InvalidResearchCitation,
    ResearchService,
    ResearchUnavailable,
    ResearchUpstreamError,
)
from api.deps import get_research_service
from api.schemas import (
    ResearchCitation,
    ResearchRequest,
    ResearchResponse,
    SourceRef,
)
from core.logger import get_logger


router = APIRouter(prefix="/api/research", tags=["research"])
logger = get_logger(__name__)


@router.post("", response_model=ResearchResponse)
async def research(
    request: ResearchRequest,
    response: Response,
    service: ResearchService = Depends(get_research_service),
) -> ResearchResponse:
    try:
        result = await service.research(request.query)
    except ResearchUnavailable as exc:
        headers = {"X-Request-ID": exc.request_id} if exc.request_id else None
        raise HTTPException(
            status_code=503,
            detail=exc.public_detail(),
            headers=headers,
        ) from exc
    except (ResearchUpstreamError, InvalidResearchCitation) as exc:
        headers = {"X-Request-ID": exc.request_id} if exc.request_id else None
        raise HTTPException(
            status_code=502,
            detail=exc.public_detail(),
            headers=headers,
        ) from exc
    if result is None:
        request_id = uuid4().hex
        reason = "Research service returned None instead of ResearchResult"
        logger.error(
            "Research endpoint failed request_id=%s error_type=EmptyServiceResult "
            "reason=%s",
            request_id,
            reason,
        )
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Research service returned no result",
                "error_type": "EmptyServiceResult",
                "reason": reason,
                "request_id": request_id,
            },
            headers={"X-Request-ID": request_id},
        )
    if result.request_id:
        response.headers["X-Request-ID"] = result.request_id
    return ResearchResponse(
        answer=result.answer,
        citations=[
            ResearchCitation(
                article_id=citation.article_id,
                title=citation.title,
                url=citation.url,
                source=(
                    SourceRef(**citation.source) if citation.source is not None else None
                ),
                published_at=citation.published_at,
            )
            for citation in result.citations
        ],
    )
